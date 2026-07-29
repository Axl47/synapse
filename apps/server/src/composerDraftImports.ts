import { createHash, randomUUID } from "node:crypto";

import {
  ComposerDraftImportPresets,
  ProjectId,
  type ComposerDraftImportCancelResult,
  type ComposerDraftImportClaimResult,
  type ComposerDraftImportCompleteInput,
  type ComposerDraftImportCreateInput,
  type ComposerDraftImportCreateResult,
  type ComposerDraftImportStatusEvent,
  type ComposerDraftImportStatusResult,
} from "@synara/contracts";
import {
  Duration,
  Effect,
  Layer,
  Option,
  PubSub,
  Schedule,
  Schema,
  ServiceMap,
  Stream,
} from "effect";

import { ServerConfig } from "./config";
import { reserveManagedAttachmentUpload } from "./managedAttachmentStore";
import type { ManagedAttachmentPrincipal } from "./managedAttachmentPrincipal";
import { ProjectionSnapshotQuery } from "./orchestration/Services/ProjectionSnapshotQuery";
import { ComposerDraftImportRepository } from "./persistence/Services/ComposerDraftImports";
import {
  ManagedAttachmentRepository,
  type ManagedAttachmentBlob,
} from "./persistence/Services/ManagedAttachments";

const IMPORT_TTL_MS = 24 * 60 * 60 * 1_000;
const CLAIM_LEASE_MS = 2 * 60 * 1_000;
const RECOVERY_INTERVAL_MS = 5 * 60 * 1_000;
const UPLOAD_ROUTE_PREFIX = "/composer-draft-imports";

export class ComposerDraftImportError extends Error {
  readonly code: string;
  readonly status: number;
  readonly retryable: boolean;

  constructor(
    message: string,
    options: { readonly code: string; readonly status?: number; readonly retryable?: boolean },
  ) {
    super(message);
    this.name = "ComposerDraftImportError";
    this.code = options.code;
    this.status = options.status ?? 400;
    this.retryable = options.retryable ?? false;
  }
}

export interface ComposerDraftImportUploadReservation {
  readonly importId: string;
  readonly attachmentId: string;
  readonly expectedBytes: number;
  readonly reservation: ManagedAttachmentBlob;
}

export interface ComposerDraftImportDownload {
  readonly importId: string;
  readonly attachmentId: string;
  readonly name: string;
  readonly mimeType: string;
  readonly sizeBytes: number;
  readonly sha256: string;
  readonly relativePath: string;
}

export interface ComposerDraftImportsShape {
  readonly create: (
    input: ComposerDraftImportCreateInput,
    principal: ManagedAttachmentPrincipal,
  ) => Effect.Effect<ComposerDraftImportCreateResult, ComposerDraftImportError>;
  readonly commit: (
    importId: string,
  ) => Effect.Effect<
    {
      readonly importId: string;
      readonly draftThreadId: string;
      readonly status: "ready";
      readonly activationUrl: string;
      readonly expiresAt: string;
    },
    ComposerDraftImportError
  >;
  readonly getStatus: (
    importId: string,
  ) => Effect.Effect<ComposerDraftImportStatusResult, ComposerDraftImportError>;
  readonly subscribeStatus: (
    importId: string,
  ) => Effect.Effect<Stream.Stream<ComposerDraftImportStatusEvent>, ComposerDraftImportError>;
  readonly claim: (
    importId: string,
  ) => Effect.Effect<ComposerDraftImportClaimResult, ComposerDraftImportError>;
  readonly complete: (
    input: ComposerDraftImportCompleteInput,
  ) => Effect.Effect<ComposerDraftImportStatusResult, ComposerDraftImportError>;
  readonly cancel: (
    importId: string,
  ) => Effect.Effect<ComposerDraftImportCancelResult, ComposerDraftImportError>;
  readonly reserveUpload: (
    input: {
      readonly importId: string;
      readonly attachmentId: string;
      readonly principal: ManagedAttachmentPrincipal;
    },
  ) => Effect.Effect<ComposerDraftImportUploadReservation, ComposerDraftImportError>;
  readonly inspectUpload: (
    importId: string,
    attachmentId: string,
  ) => Effect.Effect<
    {
      readonly kind: "image" | "file";
      readonly name: string;
      readonly mimeType: string;
      readonly expectedBytes: number;
    },
    ComposerDraftImportError
  >;
  readonly resolveDownload: (
    importId: string,
    attachmentId: string,
  ) => Effect.Effect<ComposerDraftImportDownload, ComposerDraftImportError>;
}

export class ComposerDraftImports extends ServiceMap.Service<
  ComposerDraftImports,
  ComposerDraftImportsShape
>()("synara/ComposerDraftImports") {}

function opaqueId(prefix: "cdi" | "cdl"): string {
  return `${prefix}_${randomUUID().replaceAll("-", "")}`;
}

function sha256(value: string): string {
  return createHash("sha256").update(value).digest("hex");
}

function stableValue(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(stableValue);
  if (value === null || typeof value !== "object") return value;
  return Object.fromEntries(
    Object.entries(value as Record<string, unknown>)
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([key, entry]) => [key, stableValue(entry)]),
  );
}

function payloadFingerprint(input: ComposerDraftImportCreateInput): string {
  return sha256(JSON.stringify(stableValue(input)));
}

function uploadPath(importId: string, attachmentId: string): string {
  return `${UPLOAD_ROUTE_PREFIX}/${encodeURIComponent(importId)}/attachments/${encodeURIComponent(attachmentId)}`;
}

function downloadPath(importId: string, attachmentId: string): string {
  return `${uploadPath(importId, attachmentId)}/content`;
}

function statusResult(record: {
  readonly importId: string;
  readonly draftThreadId: string;
  readonly status: ComposerDraftImportStatusResult["status"];
  readonly updatedAt: string;
  readonly expiresAt: string;
  readonly completedAt: string | null;
  readonly failureMessage: string | null;
}): ComposerDraftImportStatusResult {
  return {
    importId: record.importId as ComposerDraftImportStatusResult["importId"],
    draftThreadId: record.draftThreadId as ComposerDraftImportStatusResult["draftThreadId"],
    status: record.status,
    updatedAt: record.updatedAt,
    expiresAt: record.expiresAt,
    ...(record.completedAt ? { completedAt: record.completedAt } : {}),
    ...(record.failureMessage ? { message: record.failureMessage } : {}),
  };
}

function invalidState(message: string, code: string, retryable = false) {
  return new ComposerDraftImportError(message, { code, status: 409, retryable });
}

export const ComposerDraftImportsLive = Layer.effect(
  ComposerDraftImports,
  Effect.gen(function* () {
    const config = yield* ServerConfig;
    const managedAttachments = yield* ManagedAttachmentRepository;
    const projection = yield* ProjectionSnapshotQuery;
    const repository = yield* ComposerDraftImportRepository;
    const events = yield* PubSub.unbounded<ComposerDraftImportStatusEvent>();

    const publishRecord = (record: Parameters<typeof statusResult>[0]) =>
      PubSub.publish(events, statusResult(record));

    const recover = repository.recover({ now: new Date().toISOString() }).pipe(
      Effect.tap((records) => Effect.forEach(records, publishRecord, { discard: true })),
      Effect.catch((error) =>
        Effect.logWarning("composer draft import recovery failed", {
          error: String(error),
        }),
      ),
    );
    yield* recover;
    yield* Effect.forkScoped(
      recover.pipe(Effect.repeat(Schedule.spaced(Duration.millis(RECOVERY_INTERVAL_MS)))),
    );

    const getRecord = (importId: string) =>
      repository.findById(importId).pipe(
        Effect.mapError(
          () =>
            new ComposerDraftImportError("Could not load the draft import.", {
              code: "IMPORT_STORAGE_FAILED",
              status: 500,
              retryable: true,
            }),
        ),
        Effect.flatMap(
          Option.match({
            onNone: () =>
              Effect.fail(
                new ComposerDraftImportError("Draft import not found.", {
                  code: "IMPORT_NOT_FOUND",
                  status: 404,
                }),
              ),
            onSome: Effect.succeed,
          }),
        ),
      );

    const getStatus: ComposerDraftImportsShape["getStatus"] = (importId) =>
      getRecord(importId).pipe(Effect.map(statusResult));

    const create: ComposerDraftImportsShape["create"] = (input, principal) =>
      Effect.gen(function* () {
        if (sha256(input.prompt) !== input.promptHash) {
          return yield* new ComposerDraftImportError("Prompt hash does not match prompt text.", {
            code: "PROMPT_HASH_MISMATCH",
          });
        }
        const ids = input.attachments.map((attachment) => attachment.id);
        if (new Set(ids).size !== ids.length) {
          return yield* new ComposerDraftImportError("Attachment IDs must be unique.", {
            code: "DUPLICATE_ATTACHMENT_ID",
          });
        }
        const orders = input.attachments.map((attachment) => attachment.order).toSorted();
        if (orders.some((order, index) => order !== index)) {
          return yield* new ComposerDraftImportError(
            "Attachment order must be contiguous and start at zero.",
            { code: "INVALID_ATTACHMENT_ORDER" },
          );
        }
        const project = yield* projection
          .getProjectShellById(ProjectId.makeUnsafe(input.projectId))
          .pipe(
            Effect.mapError(
              () =>
                new ComposerDraftImportError("Could not validate the target project.", {
                  code: "PROJECT_LOOKUP_FAILED",
                  status: 500,
                  retryable: true,
                }),
            ),
          );
        if (Option.isNone(project)) {
          return yield* new ComposerDraftImportError("Target project does not exist.", {
            code: "PROJECT_NOT_FOUND",
            status: 404,
          });
        }

        const now = new Date();
        const created = yield* repository
          .create({
            importId: opaqueId("cdi"),
            idempotencyKey: input.idempotencyKey,
            payloadFingerprint: payloadFingerprint(input),
            source: input.source,
            promptKind: input.promptKind,
            projectId: input.projectId,
            draftThreadId: `draft_${randomUUID().replaceAll("-", "")}`,
            title: input.title,
            prompt: input.prompt,
            promptHash: input.promptHash,
            presets: input.presets,
            attachments: input.attachments,
            expiresAt: new Date(now.getTime() + IMPORT_TTL_MS).toISOString(),
            now: now.toISOString(),
          })
          .pipe(
            Effect.mapError(
              () =>
                new ComposerDraftImportError("Could not persist the draft import.", {
                  code: "IMPORT_STORAGE_FAILED",
                  status: 500,
                  retryable: true,
                }),
            ),
          );
        if (created.status === "idempotency-conflict") {
          return yield* invalidState(
            "The idempotency key was already used for a different payload.",
            "IDEMPOTENCY_CONFLICT",
          );
        }
        const record = created.record;
        return {
          importId: record.importId as ComposerDraftImportCreateResult["importId"],
          draftThreadId:
            record.draftThreadId as ComposerDraftImportCreateResult["draftThreadId"],
          status: record.status,
          uploadTargets: input.attachments.map((attachment) => ({
            attachmentId: attachment.id,
            path: uploadPath(record.importId, attachment.id),
          })),
          expiresAt: record.expiresAt,
        };
      });

    const reserveUpload: ComposerDraftImportsShape["reserveUpload"] = (input) =>
      Effect.gen(function* () {
        const record = yield* getRecord(input.importId);
        if (record.status !== "uploading") {
          return yield* invalidState(
            "Draft import is no longer accepting uploads.",
            "IMPORT_NOT_UPLOADING",
          );
        }
        const attachments = yield* repository
          .listAttachments(input.importId)
          .pipe(
            Effect.mapError(
              () =>
                new ComposerDraftImportError("Could not load import attachments.", {
                  code: "IMPORT_STORAGE_FAILED",
                  status: 500,
                  retryable: true,
                }),
            ),
          );
        const attachment = attachments.find(
          (entry) => entry.attachmentId === input.attachmentId,
        );
        if (!attachment) {
          return yield* new ComposerDraftImportError("Attachment was not declared.", {
            code: "ATTACHMENT_NOT_DECLARED",
            status: 404,
          });
        }
        if (
          attachment.managedAttachmentId &&
          attachment.managedState === "staged" &&
          attachment.sizeBytes === attachment.expectedBytes
        ) {
          const existing = yield* managedAttachments
            .findServerOwned({
              attachmentId: attachment.managedAttachmentId,
              ownerThreadId: record.draftThreadId,
              ownerKind: input.principal.ownerKind,
              ownerId: input.principal.ownerId,
              now: new Date().toISOString(),
            })
            .pipe(Effect.map(Option.getOrNull), Effect.orElseSucceed(() => null));
          if (existing) {
            return {
              importId: input.importId,
              attachmentId: input.attachmentId,
              expectedBytes: attachment.expectedBytes,
              reservation: existing,
            };
          }
        }
        if (attachment.managedAttachmentId) {
          return yield* invalidState(
            "Attachment upload is already in progress or no longer retryable.",
            "ATTACHMENT_UPLOAD_CONFLICT",
            true,
          );
        }

        const now = new Date().toISOString();
        const reservation = yield* reserveManagedAttachmentUpload({
          type: attachment.kind,
          threadId: record.draftThreadId,
          name: attachment.originalName,
          mimeType: attachment.mimeType,
          reservedBytes: attachment.expectedBytes,
          now,
          principal: input.principal,
          repository: managedAttachments,
        }).pipe(
          Effect.mapError(
            (cause) =>
              new ComposerDraftImportError(cause.message, {
                code: "ATTACHMENT_RESERVATION_FAILED",
                status: cause.status,
                retryable: true,
              }),
          ),
        );
        const attached = yield* repository
          .attachManagedBlob({
            importId: input.importId,
            attachmentId: input.attachmentId,
            managedAttachmentId: reservation.attachmentId,
            now,
          })
          .pipe(
            Effect.mapError(
              () =>
                new ComposerDraftImportError("Could not bind the attachment upload.", {
                  code: "IMPORT_STORAGE_FAILED",
                  status: 500,
                  retryable: true,
                }),
            ),
          );
        if (!attached) {
          yield* managedAttachments
            .cancelStaged({
              attachmentId: reservation.attachmentId,
              ownerKind: input.principal.ownerKind,
              ownerId: input.principal.ownerId,
              reason: "composer-draft-import-reservation-race",
              requestedAt: new Date().toISOString(),
            })
            .pipe(Effect.ignore);
          return yield* invalidState(
            "Attachment upload target changed before reservation completed.",
            "ATTACHMENT_UPLOAD_CONFLICT",
            true,
          );
        }
        return {
          importId: input.importId,
          attachmentId: input.attachmentId,
          expectedBytes: attachment.expectedBytes,
          reservation,
        };
      });

    const inspectUpload: ComposerDraftImportsShape["inspectUpload"] = (
      importId,
      attachmentId,
    ) =>
      Effect.gen(function* () {
        const record = yield* getRecord(importId);
        if (record.status !== "uploading") {
          return yield* invalidState(
            "Draft import is no longer accepting uploads.",
            "IMPORT_NOT_UPLOADING",
          );
        }
        const attachments = yield* repository.listAttachments(importId).pipe(
          Effect.mapError(
            () =>
              new ComposerDraftImportError("Could not load import attachments.", {
                code: "IMPORT_STORAGE_FAILED",
                status: 500,
                retryable: true,
              }),
          ),
        );
        const attachment = attachments.find((entry) => entry.attachmentId === attachmentId);
        if (!attachment) {
          return yield* new ComposerDraftImportError("Attachment was not declared.", {
            code: "ATTACHMENT_NOT_DECLARED",
            status: 404,
          });
        }
        return {
          kind: attachment.kind,
          name: attachment.originalName,
          mimeType: attachment.mimeType,
          expectedBytes: attachment.expectedBytes,
        };
      });

    const commit: ComposerDraftImportsShape["commit"] = (importId) =>
      Effect.gen(function* () {
        const base = config.externalActivationBaseUrl?.trim();
        if (!base) {
          return yield* new ComposerDraftImportError(
            "Desktop activation is not available on this Synara instance.",
            { code: "DESKTOP_ACTIVATION_UNAVAILABLE", status: 503, retryable: true },
          );
        }
        const now = new Date().toISOString();
        const result = yield* repository.commit({ importId, now }).pipe(
          Effect.mapError(
            () =>
              new ComposerDraftImportError("Could not commit the draft import.", {
                code: "IMPORT_STORAGE_FAILED",
                status: 500,
                retryable: true,
              }),
          ),
        );
        if (result.status !== "ready") {
          return yield* invalidState(
            result.status === "attachments-incomplete"
              ? "Every declared attachment must finish uploading before commit."
              : "Draft import cannot be committed from its current state.",
            result.status === "attachments-incomplete"
              ? "ATTACHMENTS_INCOMPLETE"
              : "IMPORT_NOT_UPLOADING",
            result.status === "attachments-incomplete",
          );
        }
        const activationUrl = new URL(
          encodeURIComponent(result.record.importId),
          base.endsWith("/") ? base : `${base}/`,
        ).toString();
        yield* publishRecord(result.record);
        return {
          importId: result.record.importId,
          draftThreadId: result.record.draftThreadId,
          status: "ready" as const,
          activationUrl,
          expiresAt: result.record.expiresAt,
        };
      });

    const claim: ComposerDraftImportsShape["claim"] = (importId) =>
      Effect.gen(function* () {
        const now = new Date();
        const result = yield* repository
          .claim({
            importId,
            leaseId: opaqueId("cdl"),
            leaseExpiresAt: new Date(now.getTime() + CLAIM_LEASE_MS).toISOString(),
            now: now.toISOString(),
          })
          .pipe(
            Effect.mapError(
              () =>
                new ComposerDraftImportError("Could not claim the draft import.", {
                  code: "IMPORT_STORAGE_FAILED",
                  status: 500,
                  retryable: true,
                }),
            ),
          );
        if (result.status !== "claimed") {
          return yield* invalidState(
            result.status === "not-found"
              ? "Draft import not found."
              : "Draft import is not ready to claim.",
            result.status === "not-found" ? "IMPORT_NOT_FOUND" : "IMPORT_NOT_READY",
            result.status === "lease-active" || result.status === "not-ready",
          );
        }
        const attachments = yield* repository.listAttachments(importId).pipe(
          Effect.mapError(
            () =>
              new ComposerDraftImportError("Could not load import attachments.", {
                code: "IMPORT_STORAGE_FAILED",
                status: 500,
                retryable: true,
              }),
          ),
        );
        if (
          attachments.some(
            (attachment) =>
              attachment.managedState !== "staged" ||
              attachment.sizeBytes !== attachment.expectedBytes ||
              !attachment.sha256 ||
              !attachment.relativePath,
          )
        ) {
          return yield* invalidState(
            "Draft import attachments are no longer available.",
            "ATTACHMENTS_UNAVAILABLE",
            true,
          );
        }
        const record = result.record;
        yield* publishRecord(record);
        return {
          importId: record.importId as ComposerDraftImportClaimResult["importId"],
          leaseId: record.leaseId as ComposerDraftImportClaimResult["leaseId"],
          leaseExpiresAt: record.leaseExpiresAt!,
          draftThreadId: record.draftThreadId as ComposerDraftImportClaimResult["draftThreadId"],
          projectId: record.projectId as ComposerDraftImportClaimResult["projectId"],
          title: record.title,
          promptKind: record.promptKind,
          prompt: record.prompt,
          promptHash: record.promptHash,
          attachments: attachments.map((attachment) => ({
            id: attachment.attachmentId,
            kind: attachment.kind,
            name: attachment.originalName,
            mimeType: attachment.mimeType,
            sizeBytes: attachment.sizeBytes!,
            order: attachment.order,
            sha256: attachment.sha256!,
            downloadPath: downloadPath(importId, attachment.attachmentId),
          })),
          presets: Schema.decodeUnknownSync(ComposerDraftImportPresets)(
            JSON.parse(record.presetsJson),
          ),
        };
      });

    const complete: ComposerDraftImportsShape["complete"] = (input) =>
      repository
        .complete({
          importId: input.importId,
          leaseId: input.leaseId,
          promptHash: input.promptHash,
          attachmentIds: input.attachmentIds,
          now: new Date().toISOString(),
        })
        .pipe(
          Effect.mapError(
            () =>
              new ComposerDraftImportError("Could not complete the draft import.", {
                code: "IMPORT_STORAGE_FAILED",
                status: 500,
                retryable: true,
              }),
          ),
          Effect.flatMap((result) =>
            result.status === "completed"
              ? publishRecord(result.record).pipe(Effect.as(statusResult(result.record)))
              : Effect.fail(
                  invalidState(
                    result.status === "verification-mismatch"
                      ? "Persisted draft verification did not match the import."
                      : "Draft import claim is no longer valid.",
                    result.status === "verification-mismatch"
                      ? "IMPORT_VERIFICATION_MISMATCH"
                      : "IMPORT_CLAIM_INVALID",
                    result.status !== "verification-mismatch",
                  ),
                ),
          ),
        );

    const cancel: ComposerDraftImportsShape["cancel"] = (importId) =>
      repository.cancel({ importId, now: new Date().toISOString() }).pipe(
        Effect.mapError(
          () =>
            new ComposerDraftImportError("Could not cancel the draft import.", {
              code: "IMPORT_STORAGE_FAILED",
              status: 500,
              retryable: true,
            }),
        ),
        Effect.flatMap((result) =>
          result.status === "not-found"
            ? Effect.fail(
                new ComposerDraftImportError("Draft import not found.", {
                  code: "IMPORT_NOT_FOUND",
                  status: 404,
                }),
              )
            : publishRecord(result.record).pipe(
                Effect.as({
                  importId: result.record.importId as ComposerDraftImportCancelResult["importId"],
                  status: result.status,
                }),
              ),
        ),
      );

    const subscribeStatus: ComposerDraftImportsShape["subscribeStatus"] = (importId) =>
      getStatus(importId).pipe(
        Effect.map((snapshot) =>
          Stream.concat(
            Stream.succeed(snapshot),
            Stream.fromPubSub(events).pipe(
              Stream.filter((event) => event.importId === importId),
            ),
          ),
        ),
      );

    const resolveDownload: ComposerDraftImportsShape["resolveDownload"] = (
      importId,
      attachmentId,
    ) =>
      Effect.gen(function* () {
        const record = yield* getRecord(importId);
        if (record.status !== "claiming") {
          return yield* invalidState(
            "Draft import attachments are available only while the import is claimed.",
            "IMPORT_NOT_CLAIMED",
          );
        }
        const attachment = (yield* repository.listAttachments(importId)).find(
          (entry) => entry.attachmentId === attachmentId,
        );
        if (
          !attachment ||
          attachment.managedState !== "staged" ||
          attachment.sizeBytes === null ||
          !attachment.sha256 ||
          !attachment.relativePath
        ) {
          return yield* new ComposerDraftImportError("Attachment not found.", {
            code: "ATTACHMENT_NOT_FOUND",
            status: 404,
          });
        }
        return {
          importId,
          attachmentId,
          name: attachment.originalName,
          mimeType: attachment.mimeType,
          sizeBytes: attachment.sizeBytes,
          sha256: attachment.sha256,
          relativePath: attachment.relativePath,
        };
      });

    return {
      create,
      commit,
      getStatus,
      subscribeStatus,
      claim,
      complete,
      cancel,
      inspectUpload,
      reserveUpload,
      resolveDownload,
    } satisfies ComposerDraftImportsShape;
  }),
);
