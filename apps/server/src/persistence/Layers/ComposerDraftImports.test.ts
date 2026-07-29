import { assert, it } from "@effect/vitest";
import { Effect, Layer } from "effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { ComposerDraftImportRepository } from "../Services/ComposerDraftImports.ts";
import { ManagedAttachmentRepository } from "../Services/ManagedAttachments.ts";
import { ComposerDraftImportRepositoryLive } from "./ComposerDraftImports.ts";
import { ManagedAttachmentRepositoryLive } from "./ManagedAttachments.ts";
import { SqlitePersistenceMemory } from "./Sqlite.ts";

const layer = it.layer(
  Layer.mergeAll(
    ComposerDraftImportRepositoryLive.pipe(Layer.provideMerge(SqlitePersistenceMemory)),
    ManagedAttachmentRepositoryLive.pipe(Layer.provideMerge(SqlitePersistenceMemory)),
    SqlitePersistenceMemory,
  ),
);

const resetSchema = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`DELETE FROM composer_draft_import_attachments`;
  yield* sql`DELETE FROM composer_draft_imports`;
  yield* sql`DELETE FROM managed_attachment_cleanup_jobs`;
  yield* sql`DELETE FROM managed_attachment_blobs`;
});

const createInput = (overrides: Partial<{ idempotencyKey: string; fingerprint: string }> = {}) => ({
  importId: "cdi_testimport",
  idempotencyKey: overrides.idempotencyKey ?? "pragma:prompt:revision",
  payloadFingerprint: overrides.fingerprint ?? "a".repeat(64),
  source: {
    app: "pragma" as const,
    promptId: "prompt-1",
    revisionId: "revision-1",
    fingerprint: "b".repeat(64),
  },
  promptKind: "reusable" as const,
  projectId: "project-1",
  draftThreadId: "draft-thread-1",
  title: "Imported prompt",
  prompt: "Review this change.",
  promptHash: "c".repeat(64),
  presets: { skills: [] },
  attachments: [
    {
      id: "asset-1",
      kind: "file" as const,
      name: "context.txt",
      mimeType: "text/plain",
      sizeBytes: 4,
      order: 0,
    },
  ],
  expiresAt: "2026-08-01T00:00:00.000Z",
  now: "2026-07-29T00:00:00.000Z",
});

layer("ComposerDraftImportRepository", (it) => {
  it.effect("keeps create idempotent and rejects key reuse for a different payload", () =>
    Effect.gen(function* () {
      yield* resetSchema;
      const repository = yield* ComposerDraftImportRepository;

      const created = yield* repository.create(createInput());
      const repeated = yield* repository.create({
        ...createInput(),
        importId: "cdi_ignoredrepeat",
        draftThreadId: "draft-thread-ignored",
      });
      const conflict = yield* repository.create({
        ...createInput({ fingerprint: "d".repeat(64) }),
        importId: "cdi_conflict",
        draftThreadId: "draft-thread-conflict",
      });

      assert.strictEqual(created.status, "created");
      assert.strictEqual(repeated.status, "existing");
      assert.strictEqual(
        repeated.status === "existing" ? repeated.record.importId : null,
        "cdi_testimport",
      );
      assert.strictEqual(conflict.status, "idempotency-conflict");
    }),
  );

  it.effect("commits only durable attachments and completes only a verified claim", () =>
    Effect.gen(function* () {
      yield* resetSchema;
      const imports = yield* ComposerDraftImportRepository;
      const attachments = yield* ManagedAttachmentRepository;
      yield* imports.create(createInput());

      assert.strictEqual(
        (yield* imports.commit({
          importId: "cdi_testimport",
          now: "2026-07-29T00:01:00.000Z",
        })).status,
        "attachments-incomplete",
      );

      const reservation = yield* attachments.reserve({
        attachmentId: "att_v2_importasset",
        ownerThreadId: "draft-thread-1",
        ownerKind: "local-loopback",
        ownerId: "local-loopback",
        kind: "file",
        originalName: "context.txt",
        mimeType: "text/plain",
        reservedBytes: 4,
        relativePath: "objects/aa/att_v2_importasset.txt",
        now: "2026-07-29T00:01:01.000Z",
      });
      assert.strictEqual(reservation.status, "reserved");
      assert.isTrue(
        yield* imports.attachManagedBlob({
          importId: "cdi_testimport",
          attachmentId: "asset-1",
          managedAttachmentId: "att_v2_importasset",
          now: "2026-07-29T00:01:02.000Z",
        }),
      );
      assert.strictEqual(
        (yield* attachments.finalizeStaged({
          attachmentId: "att_v2_importasset",
          ownerThreadId: "draft-thread-1",
          ownerKind: "local-loopback",
          ownerId: "local-loopback",
          sizeBytes: 4,
          sha256: "e".repeat(64),
          stagingExpiresAt: "2026-07-29T01:01:03.000Z",
          now: "2026-07-29T00:01:03.000Z",
        })).status,
        "staged",
      );

      assert.strictEqual(
        (yield* imports.commit({
          importId: "cdi_testimport",
          now: "2026-07-29T00:02:00.000Z",
        })).status,
        "ready",
      );
      const claim = yield* imports.claim({
        importId: "cdi_testimport",
        leaseId: "cdl_testlease",
        leaseExpiresAt: "2026-07-29T00:05:00.000Z",
        now: "2026-07-29T00:03:00.000Z",
      });
      assert.strictEqual(claim.status, "claimed");
      assert.strictEqual(
        (yield* imports.complete({
          importId: "cdi_testimport",
          leaseId: "cdl_testlease",
          promptHash: "f".repeat(64),
          attachmentIds: ["asset-1"],
          now: "2026-07-29T00:04:00.000Z",
        })).status,
        "verification-mismatch",
      );
      assert.strictEqual(
        (yield* imports.complete({
          importId: "cdi_testimport",
          leaseId: "cdl_testlease",
          promptHash: "c".repeat(64),
          attachmentIds: ["asset-1"],
          now: "2026-07-29T00:04:01.000Z",
        })).status,
        "completed",
      );
      assert.strictEqual(
        (yield* imports.complete({
          importId: "cdi_testimport",
          leaseId: "cdl_testlease",
          promptHash: "c".repeat(64),
          attachmentIds: ["asset-1"],
          now: "2026-07-29T00:04:02.000Z",
        })).status,
        "completed",
      );
    }),
  );
});
