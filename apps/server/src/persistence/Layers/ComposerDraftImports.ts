import { Effect, Layer, Option } from "effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { toPersistenceSqlError } from "../Errors.ts";
import {
  ComposerDraftImportRepository,
  type ComposerDraftImportAttachmentRecord,
  type ComposerDraftImportRecord,
  type ComposerDraftImportRepositoryShape,
} from "../Services/ComposerDraftImports.ts";

const importColumns = (sql: SqlClient.SqlClient) => sql`
  import_id AS "importId",
  idempotency_key AS "idempotencyKey",
  payload_fingerprint AS "payloadFingerprint",
  source_json AS "sourceJson",
  prompt_kind AS "promptKind",
  project_id AS "projectId",
  draft_thread_id AS "draftThreadId",
  title,
  prompt,
  prompt_hash AS "promptHash",
  presets_json AS "presetsJson",
  status,
  lease_id AS "leaseId",
  lease_expires_at AS "leaseExpiresAt",
  failure_message AS "failureMessage",
  expires_at AS "expiresAt",
  completed_at AS "completedAt",
  created_at AS "createdAt",
  updated_at AS "updatedAt"
`;

const attachmentColumns = (sql: SqlClient.SqlClient) => sql`
  mapping.import_id AS "importId",
  mapping.attachment_id AS "attachmentId",
  mapping.managed_attachment_id AS "managedAttachmentId",
  mapping.kind,
  mapping.original_name AS "originalName",
  mapping.mime_type AS "mimeType",
  mapping.expected_bytes AS "expectedBytes",
  mapping.attachment_order AS "order",
  blob.size_bytes AS "sizeBytes",
  blob.sha256,
  blob.relative_path AS "relativePath",
  blob.state AS "managedState"
`;

const makeRepository = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  const findById: ComposerDraftImportRepositoryShape["findById"] = (importId) =>
    sql<ComposerDraftImportRecord>`
      SELECT ${importColumns(sql)}
      FROM composer_draft_imports
      WHERE import_id = ${importId}
      LIMIT 1
    `.pipe(
      Effect.map((rows) => Option.fromNullishOr(rows[0])),
      Effect.mapError(toPersistenceSqlError("ComposerDraftImport.findById")),
    );

  const listAttachments: ComposerDraftImportRepositoryShape["listAttachments"] = (importId) =>
    sql<ComposerDraftImportAttachmentRecord>`
      SELECT ${attachmentColumns(sql)}
      FROM composer_draft_import_attachments AS mapping
      LEFT JOIN managed_attachment_blobs AS blob
        ON blob.attachment_id = mapping.managed_attachment_id
      WHERE mapping.import_id = ${importId}
      ORDER BY mapping.attachment_order ASC
    `.pipe(Effect.mapError(toPersistenceSqlError("ComposerDraftImport.listAttachments")));

  const enqueueMappedAttachmentCleanup = (input: {
    readonly importId: string;
    readonly reason: string;
    readonly now: string;
  }) =>
    Effect.gen(function* () {
      yield* sql`
        UPDATE managed_attachment_blobs
        SET state = 'deleting',
            delete_reason = ${input.reason},
            delete_requested_at = ${input.now},
            updated_at = ${input.now}
        WHERE attachment_id IN (
          SELECT managed_attachment_id
          FROM composer_draft_import_attachments
          WHERE import_id = ${input.importId}
            AND managed_attachment_id IS NOT NULL
        )
          AND state IN ('uploading', 'staged')
      `;
      yield* sql`
        INSERT INTO managed_attachment_cleanup_jobs (
          attachment_id, reason, attempt_count, next_attempt_at,
          lease_owner, lease_expires_at, last_error, created_at, updated_at
        )
        SELECT attachment_id, ${input.reason}, 0, ${input.now},
               NULL, NULL, NULL, ${input.now}, ${input.now}
        FROM managed_attachment_blobs
        WHERE attachment_id IN (
          SELECT managed_attachment_id
          FROM composer_draft_import_attachments
          WHERE import_id = ${input.importId}
            AND managed_attachment_id IS NOT NULL
        )
          AND state = 'deleting'
        ON CONFLICT (attachment_id) DO NOTHING
      `;
    });

  const create: ComposerDraftImportRepositoryShape["create"] = (input) =>
    sql
      .withTransaction(
        Effect.gen(function* () {
          const inserted = yield* sql<ComposerDraftImportRecord>`
            INSERT INTO composer_draft_imports (
              import_id, idempotency_key, payload_fingerprint, source_json,
              prompt_kind, project_id, draft_thread_id, title, prompt, prompt_hash,
              presets_json, status, lease_id, lease_expires_at, failure_message,
              expires_at, completed_at, created_at, updated_at
            )
            VALUES (
              ${input.importId}, ${input.idempotencyKey}, ${input.payloadFingerprint},
              ${JSON.stringify(input.source)}, ${input.promptKind}, ${input.projectId},
              ${input.draftThreadId}, ${input.title}, ${input.prompt}, ${input.promptHash},
              ${JSON.stringify(input.presets)}, 'uploading', NULL, NULL, NULL,
              ${input.expiresAt}, NULL, ${input.now}, ${input.now}
            )
            ON CONFLICT (idempotency_key) DO NOTHING
            RETURNING ${importColumns(sql)}
          `;
          if (inserted[0]) {
            for (const attachment of input.attachments) {
              yield* sql`
                INSERT INTO composer_draft_import_attachments (
                  import_id, attachment_id, managed_attachment_id, kind, original_name,
                  mime_type, expected_bytes, attachment_order, created_at, updated_at
                )
                VALUES (
                  ${input.importId}, ${attachment.id}, NULL, ${attachment.kind},
                  ${attachment.name}, ${attachment.mimeType}, ${attachment.sizeBytes},
                  ${attachment.order}, ${input.now}, ${input.now}
                )
              `;
            }
            return { status: "created" as const, record: inserted[0] };
          }

          const existing = yield* sql<ComposerDraftImportRecord>`
            SELECT ${importColumns(sql)}
            FROM composer_draft_imports
            WHERE idempotency_key = ${input.idempotencyKey}
            LIMIT 1
          `;
          if (existing[0]?.payloadFingerprint === input.payloadFingerprint) {
            return { status: "existing" as const, record: existing[0] };
          }
          return { status: "idempotency-conflict" as const };
        }),
      )
      .pipe(Effect.mapError(toPersistenceSqlError("ComposerDraftImport.create")));

  const attachManagedBlob: ComposerDraftImportRepositoryShape["attachManagedBlob"] = (input) =>
    sql<{ readonly attachmentId: string }>`
      UPDATE composer_draft_import_attachments
      SET managed_attachment_id = ${input.managedAttachmentId},
          updated_at = ${input.now}
      WHERE import_id = ${input.importId}
        AND attachment_id = ${input.attachmentId}
        AND (
          managed_attachment_id IS NULL
          OR managed_attachment_id = ${input.managedAttachmentId}
          OR EXISTS (
            SELECT 1
            FROM managed_attachment_blobs AS previous_blob
            WHERE previous_blob.attachment_id =
              composer_draft_import_attachments.managed_attachment_id
              AND previous_blob.state IN ('deleting', 'deleted')
          )
        )
        AND EXISTS (
          SELECT 1
          FROM composer_draft_imports
          WHERE import_id = ${input.importId}
            AND status = 'uploading'
        )
      RETURNING attachment_id AS "attachmentId"
    `.pipe(
      Effect.map((rows) => rows.length === 1),
      Effect.mapError(toPersistenceSqlError("ComposerDraftImport.attachManagedBlob")),
    );

  const commit: ComposerDraftImportRepositoryShape["commit"] = (input) =>
    sql
      .withTransaction(
        Effect.gen(function* () {
          const currentOption = yield* findById(input.importId);
          if (Option.isNone(currentOption)) return { status: "not-found" as const };
          const current = currentOption.value;
          if (
            current.status === "ready" ||
            current.status === "claiming" ||
            current.status === "completed"
          ) {
            return { status: "ready" as const, record: current };
          }
          if (current.status !== "uploading") return { status: "not-uploading" as const };

          const counts = yield* sql<{
            readonly declaredCount: number;
            readonly completeCount: number;
          }>`
            SELECT
              COUNT(*) AS "declaredCount",
              COALESCE(SUM(
                CASE
                  WHEN blob.state = 'staged'
                    AND blob.size_bytes = mapping.expected_bytes
                    AND blob.sha256 IS NOT NULL
                  THEN 1
                  ELSE 0
                END
              ), 0) AS "completeCount"
            FROM composer_draft_import_attachments AS mapping
            LEFT JOIN managed_attachment_blobs AS blob
              ON blob.attachment_id = mapping.managed_attachment_id
            WHERE mapping.import_id = ${input.importId}
          `;
          const count = counts[0];
          if (!count || Number(count.declaredCount) !== Number(count.completeCount)) {
            return { status: "attachments-incomplete" as const };
          }
          const updated = yield* sql<ComposerDraftImportRecord>`
            UPDATE composer_draft_imports
            SET status = 'ready',
                updated_at = ${input.now}
            WHERE import_id = ${input.importId}
              AND status = 'uploading'
            RETURNING ${importColumns(sql)}
          `;
          return updated[0]
            ? { status: "ready" as const, record: updated[0] }
            : { status: "not-uploading" as const };
        }),
      )
      .pipe(Effect.mapError(toPersistenceSqlError("ComposerDraftImport.commit")));

  const claim: ComposerDraftImportRepositoryShape["claim"] = (input) =>
    sql
      .withTransaction(
        Effect.gen(function* () {
          const currentOption = yield* findById(input.importId);
          if (Option.isNone(currentOption)) return { status: "not-found" as const };
          let current = currentOption.value;
          if (current.expiresAt <= input.now && current.status !== "completed") {
            const expired = yield* sql<ComposerDraftImportRecord>`
              UPDATE composer_draft_imports
              SET status = 'expired',
                  lease_id = NULL,
                  lease_expires_at = NULL,
                  updated_at = ${input.now}
              WHERE import_id = ${input.importId}
                AND status IN ('uploading', 'ready', 'claiming')
              RETURNING ${importColumns(sql)}
            `;
            if (expired[0]) {
              yield* enqueueMappedAttachmentCleanup({
                importId: input.importId,
                reason: "composer-draft-import-expired",
                now: input.now,
              });
            }
            return { status: "expired" as const };
          }
          if (
            current.status === "completed" ||
            current.status === "failed" ||
            current.status === "cancelled" ||
            current.status === "expired"
          ) {
            return { status: "terminal" as const };
          }
          if (current.status === "claiming" && current.leaseExpiresAt! > input.now) {
            return { status: "claimed" as const, record: current };
          }
          if (current.status === "claiming") {
            const released = yield* sql<ComposerDraftImportRecord>`
              UPDATE composer_draft_imports
              SET status = 'ready',
                  lease_id = NULL,
                  lease_expires_at = NULL,
                  updated_at = ${input.now}
              WHERE import_id = ${input.importId}
                AND status = 'claiming'
                AND lease_expires_at <= ${input.now}
              RETURNING ${importColumns(sql)}
            `;
            if (!released[0]) return { status: "lease-active" as const };
            current = released[0];
          }
          if (current.status !== "ready") return { status: "not-ready" as const };
          const claimed = yield* sql<ComposerDraftImportRecord>`
            UPDATE composer_draft_imports
            SET status = 'claiming',
                lease_id = ${input.leaseId},
                lease_expires_at = ${input.leaseExpiresAt},
                updated_at = ${input.now}
            WHERE import_id = ${input.importId}
              AND status = 'ready'
            RETURNING ${importColumns(sql)}
          `;
          return claimed[0]
            ? { status: "claimed" as const, record: claimed[0] }
            : { status: "not-ready" as const };
        }),
      )
      .pipe(Effect.mapError(toPersistenceSqlError("ComposerDraftImport.claim")));

  const complete: ComposerDraftImportRepositoryShape["complete"] = (input) =>
    sql
      .withTransaction(
        Effect.gen(function* () {
          const currentOption = yield* findById(input.importId);
          if (Option.isNone(currentOption)) return { status: "not-found" as const };
          const current = currentOption.value;
          if (current.status === "completed") {
            return { status: "completed" as const, record: current };
          }
          if (current.status !== "claiming") return { status: "not-claiming" as const };
          if (current.leaseId !== input.leaseId || current.leaseExpiresAt! <= input.now) {
            return { status: "lease-mismatch" as const };
          }
          const attachments = yield* listAttachments(input.importId);
          const expectedIds = attachments.map((attachment) => attachment.attachmentId);
          if (
            current.promptHash !== input.promptHash ||
            expectedIds.length !== input.attachmentIds.length ||
            expectedIds.some((id, index) => id !== input.attachmentIds[index])
          ) {
            return { status: "verification-mismatch" as const };
          }
          const completed = yield* sql<ComposerDraftImportRecord>`
            UPDATE composer_draft_imports
            SET status = 'completed',
                lease_id = NULL,
                lease_expires_at = NULL,
                completed_at = ${input.now},
                updated_at = ${input.now}
            WHERE import_id = ${input.importId}
              AND status = 'claiming'
              AND lease_id = ${input.leaseId}
            RETURNING ${importColumns(sql)}
          `;
          if (!completed[0]) return { status: "lease-mismatch" as const };
          yield* enqueueMappedAttachmentCleanup({
            importId: input.importId,
            reason: "composer-draft-import-completed",
            now: input.now,
          });
          return { status: "completed" as const, record: completed[0] };
        }),
      )
      .pipe(Effect.mapError(toPersistenceSqlError("ComposerDraftImport.complete")));

  const cancel: ComposerDraftImportRepositoryShape["cancel"] = (input) =>
    sql
      .withTransaction(
        Effect.gen(function* () {
          const currentOption = yield* findById(input.importId);
          if (Option.isNone(currentOption)) return { status: "not-found" as const };
          const current = currentOption.value;
          if (current.status === "completed") {
            return { status: "completed" as const, record: current };
          }
          if (
            current.status === "cancelled" ||
            current.status === "failed" ||
            current.status === "expired"
          ) {
            return { status: "cancelled" as const, record: current };
          }
          const cancelled = yield* sql<ComposerDraftImportRecord>`
            UPDATE composer_draft_imports
            SET status = 'cancelled',
                lease_id = NULL,
                lease_expires_at = NULL,
                updated_at = ${input.now}
            WHERE import_id = ${input.importId}
              AND status IN ('uploading', 'ready', 'claiming')
            RETURNING ${importColumns(sql)}
          `;
          const next = cancelled[0] ?? current;
          yield* enqueueMappedAttachmentCleanup({
            importId: input.importId,
            reason: "composer-draft-import-cancelled",
            now: input.now,
          });
          return { status: "cancelled" as const, record: next };
        }),
      )
      .pipe(Effect.mapError(toPersistenceSqlError("ComposerDraftImport.cancel")));

  const recover: ComposerDraftImportRepositoryShape["recover"] = (input) =>
    sql
      .withTransaction(
        Effect.gen(function* () {
          const released = yield* sql<ComposerDraftImportRecord>`
            UPDATE composer_draft_imports
            SET status = 'ready',
                lease_id = NULL,
                lease_expires_at = NULL,
                updated_at = ${input.now}
            WHERE status = 'claiming'
              AND lease_expires_at <= ${input.now}
              AND expires_at > ${input.now}
            RETURNING ${importColumns(sql)}
          `;
          const expired = yield* sql<ComposerDraftImportRecord>`
            UPDATE composer_draft_imports
            SET status = 'expired',
                lease_id = NULL,
                lease_expires_at = NULL,
                updated_at = ${input.now}
            WHERE status IN ('uploading', 'ready', 'claiming')
              AND expires_at <= ${input.now}
            RETURNING ${importColumns(sql)}
          `;
          for (const record of expired) {
            yield* enqueueMappedAttachmentCleanup({
              importId: record.importId,
              reason: "composer-draft-import-expired",
              now: input.now,
            });
          }
          return [...released, ...expired];
        }),
      )
      .pipe(Effect.mapError(toPersistenceSqlError("ComposerDraftImport.recover")));

  return {
    create,
    findById,
    listAttachments,
    attachManagedBlob,
    commit,
    claim,
    complete,
    cancel,
    recover,
  } satisfies ComposerDraftImportRepositoryShape;
});

export const ComposerDraftImportRepositoryLive = Layer.effect(
  ComposerDraftImportRepository,
  makeRepository,
);
