import { Schema } from "effect";

import {
  IsoDateTime,
  NonNegativeInt,
  ProjectId,
  ThreadId,
  TrimmedNonEmptyString,
} from "./baseSchemas";
import {
  ModelSelection,
  PROVIDER_SEND_TURN_MAX_ATTACHMENTS,
  PROVIDER_SEND_TURN_MAX_FILE_BYTES,
  PROVIDER_SEND_TURN_MAX_IMAGE_BYTES,
  PROVIDER_SEND_TURN_MAX_INPUT_CHARS,
  ProviderInteractionMode,
  ThreadEnvironmentMode,
} from "./orchestration";
import { ProviderSkillReference } from "./providerDiscovery";

const OpaqueImportIdString = TrimmedNonEmptyString.check(
  Schema.isMaxLength(128),
  Schema.isPattern(/^cdi_[a-z0-9]+$/),
);
export const ComposerDraftImportId = OpaqueImportIdString.pipe(
  Schema.brand("ComposerDraftImportId"),
);
export type ComposerDraftImportId = typeof ComposerDraftImportId.Type;

export const ComposerDraftImportLeaseId = TrimmedNonEmptyString.check(
  Schema.isMaxLength(128),
  Schema.isPattern(/^cdl_[a-z0-9]+$/),
).pipe(Schema.brand("ComposerDraftImportLeaseId"));
export type ComposerDraftImportLeaseId = typeof ComposerDraftImportLeaseId.Type;

export const ComposerDraftImportStatus = Schema.Literals([
  "uploading",
  "ready",
  "claiming",
  "completed",
  "failed",
  "cancelled",
  "expired",
]);
export type ComposerDraftImportStatus = typeof ComposerDraftImportStatus.Type;

export const ComposerDraftImportPromptKind = Schema.Literals(["oneOff", "reusable"]);
export type ComposerDraftImportPromptKind = typeof ComposerDraftImportPromptKind.Type;

const Sha256 = TrimmedNonEmptyString.check(
  Schema.isMinLength(64),
  Schema.isMaxLength(64),
  Schema.isPattern(/^[0-9a-f]+$/),
);

export const ComposerDraftImportSource = Schema.Struct({
  app: Schema.Literal("pragma"),
  promptId: TrimmedNonEmptyString.check(Schema.isMaxLength(256)),
  revisionId: TrimmedNonEmptyString.check(Schema.isMaxLength(256)),
  fingerprint: Sha256,
});
export type ComposerDraftImportSource = typeof ComposerDraftImportSource.Type;

const ComposerDraftImportAttachmentId = TrimmedNonEmptyString.check(
  Schema.isMaxLength(128),
  Schema.isPattern(/^[a-z0-9_-]+$/i),
);

const ComposerDraftImportAttachmentBase = {
  id: ComposerDraftImportAttachmentId,
  name: TrimmedNonEmptyString.check(Schema.isMaxLength(255)),
  mimeType: TrimmedNonEmptyString.check(Schema.isMaxLength(100)),
  order: NonNegativeInt,
} as const;

export const ComposerDraftImportImageAttachmentManifest = Schema.Struct({
  ...ComposerDraftImportAttachmentBase,
  kind: Schema.Literal("image"),
  mimeType: ComposerDraftImportAttachmentBase.mimeType.check(Schema.isPattern(/^image\//i)),
  sizeBytes: NonNegativeInt.check(
    Schema.isGreaterThan(0),
    Schema.isLessThanOrEqualTo(PROVIDER_SEND_TURN_MAX_IMAGE_BYTES),
  ),
});
export const ComposerDraftImportFileAttachmentManifest = Schema.Struct({
  ...ComposerDraftImportAttachmentBase,
  kind: Schema.Literal("file"),
  sizeBytes: NonNegativeInt.check(
    Schema.isGreaterThan(0),
    Schema.isLessThanOrEqualTo(PROVIDER_SEND_TURN_MAX_FILE_BYTES),
  ),
});
export const ComposerDraftImportAttachmentManifest = Schema.Union([
  ComposerDraftImportImageAttachmentManifest,
  ComposerDraftImportFileAttachmentManifest,
]);
export type ComposerDraftImportAttachmentManifest =
  typeof ComposerDraftImportAttachmentManifest.Type;

export const ComposerDraftImportPresets = Schema.Struct({
  modelSelection: Schema.optional(ModelSelection),
  skills: Schema.Array(ProviderSkillReference).check(Schema.isMaxLength(64)),
  interactionMode: Schema.optional(ProviderInteractionMode),
  environment: Schema.optional(ThreadEnvironmentMode),
});
export type ComposerDraftImportPresets = typeof ComposerDraftImportPresets.Type;

export const ComposerDraftImportCreateInput = Schema.Struct({
  idempotencyKey: TrimmedNonEmptyString.check(Schema.isMaxLength(512)),
  source: ComposerDraftImportSource,
  promptKind: ComposerDraftImportPromptKind,
  projectId: ProjectId,
  title: TrimmedNonEmptyString.check(Schema.isMaxLength(255)),
  prompt: Schema.String.check(Schema.isMaxLength(PROVIDER_SEND_TURN_MAX_INPUT_CHARS)),
  promptHash: Sha256,
  attachments: Schema.Array(ComposerDraftImportAttachmentManifest).check(
    Schema.isMaxLength(PROVIDER_SEND_TURN_MAX_ATTACHMENTS),
  ),
  presets: ComposerDraftImportPresets,
});
export type ComposerDraftImportCreateInput = typeof ComposerDraftImportCreateInput.Type;

export const ComposerDraftImportUploadTarget = Schema.Struct({
  attachmentId: ComposerDraftImportAttachmentId,
  path: TrimmedNonEmptyString.check(Schema.isMaxLength(512)),
});
export type ComposerDraftImportUploadTarget = typeof ComposerDraftImportUploadTarget.Type;

export const ComposerDraftImportCreateResult = Schema.Struct({
  importId: ComposerDraftImportId,
  draftThreadId: ThreadId,
  status: ComposerDraftImportStatus,
  uploadTargets: Schema.Array(ComposerDraftImportUploadTarget),
  expiresAt: IsoDateTime,
});
export type ComposerDraftImportCreateResult = typeof ComposerDraftImportCreateResult.Type;

export const ComposerDraftImportIdInput = Schema.Struct({
  importId: ComposerDraftImportId,
});
export type ComposerDraftImportIdInput = typeof ComposerDraftImportIdInput.Type;

export const ComposerDraftImportCommitResult = Schema.Struct({
  importId: ComposerDraftImportId,
  draftThreadId: ThreadId,
  status: Schema.Literal("ready"),
  activationUrl: TrimmedNonEmptyString.check(Schema.isMaxLength(512)),
  expiresAt: IsoDateTime,
});
export type ComposerDraftImportCommitResult = typeof ComposerDraftImportCommitResult.Type;

export const ComposerDraftImportStatusResult = Schema.Struct({
  importId: ComposerDraftImportId,
  draftThreadId: ThreadId,
  status: ComposerDraftImportStatus,
  updatedAt: IsoDateTime,
  expiresAt: IsoDateTime,
  completedAt: Schema.optional(IsoDateTime),
  message: Schema.optional(TrimmedNonEmptyString.check(Schema.isMaxLength(500))),
});
export type ComposerDraftImportStatusResult = typeof ComposerDraftImportStatusResult.Type;
export const ComposerDraftImportStatusEvent = ComposerDraftImportStatusResult;
export type ComposerDraftImportStatusEvent = typeof ComposerDraftImportStatusEvent.Type;

export const ComposerDraftImportClaimAttachment = Schema.Struct({
  ...ComposerDraftImportAttachmentBase,
  kind: Schema.Literals(["image", "file"]),
  sizeBytes: NonNegativeInt,
  sha256: Sha256,
  downloadPath: TrimmedNonEmptyString.check(Schema.isMaxLength(512)),
});
export type ComposerDraftImportClaimAttachment = typeof ComposerDraftImportClaimAttachment.Type;

export const ComposerDraftImportClaimResult = Schema.Struct({
  importId: ComposerDraftImportId,
  leaseId: ComposerDraftImportLeaseId,
  leaseExpiresAt: IsoDateTime,
  draftThreadId: ThreadId,
  projectId: ProjectId,
  title: TrimmedNonEmptyString.check(Schema.isMaxLength(255)),
  promptKind: ComposerDraftImportPromptKind,
  prompt: Schema.String.check(Schema.isMaxLength(PROVIDER_SEND_TURN_MAX_INPUT_CHARS)),
  promptHash: Sha256,
  attachments: Schema.Array(ComposerDraftImportClaimAttachment).check(
    Schema.isMaxLength(PROVIDER_SEND_TURN_MAX_ATTACHMENTS),
  ),
  presets: ComposerDraftImportPresets,
});
export type ComposerDraftImportClaimResult = typeof ComposerDraftImportClaimResult.Type;

export const ComposerDraftImportCompleteInput = Schema.Struct({
  importId: ComposerDraftImportId,
  leaseId: ComposerDraftImportLeaseId,
  promptHash: Sha256,
  attachmentIds: Schema.Array(ComposerDraftImportAttachmentId).check(
    Schema.isMaxLength(PROVIDER_SEND_TURN_MAX_ATTACHMENTS),
  ),
});
export type ComposerDraftImportCompleteInput = typeof ComposerDraftImportCompleteInput.Type;

export const ComposerDraftImportCancelResult = Schema.Struct({
  importId: ComposerDraftImportId,
  status: Schema.Literals(["cancelled", "completed"]),
});
export type ComposerDraftImportCancelResult = typeof ComposerDraftImportCancelResult.Type;
