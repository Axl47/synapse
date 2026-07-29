import type {
  ComposerDraftImportAttachmentManifest,
  ComposerDraftImportPresets,
  ComposerDraftImportPromptKind,
  ComposerDraftImportSource,
  ComposerDraftImportStatus,
} from "@synara/contracts";
import { Option, ServiceMap } from "effect";
import type { Effect } from "effect";

import type { PersistenceSqlError } from "../Errors.ts";

export interface ComposerDraftImportRecord {
  readonly importId: string;
  readonly idempotencyKey: string;
  readonly payloadFingerprint: string;
  readonly sourceJson: string;
  readonly promptKind: ComposerDraftImportPromptKind;
  readonly projectId: string;
  readonly draftThreadId: string;
  readonly title: string;
  readonly prompt: string;
  readonly promptHash: string;
  readonly presetsJson: string;
  readonly status: ComposerDraftImportStatus;
  readonly leaseId: string | null;
  readonly leaseExpiresAt: string | null;
  readonly failureMessage: string | null;
  readonly expiresAt: string;
  readonly completedAt: string | null;
  readonly createdAt: string;
  readonly updatedAt: string;
}

export interface ComposerDraftImportAttachmentRecord {
  readonly importId: string;
  readonly attachmentId: string;
  readonly managedAttachmentId: string | null;
  readonly kind: "image" | "file";
  readonly originalName: string;
  readonly mimeType: string;
  readonly expectedBytes: number;
  readonly order: number;
  readonly sizeBytes: number | null;
  readonly sha256: string | null;
  readonly relativePath: string | null;
  readonly managedState: string | null;
}

export type CreateComposerDraftImportResult =
  | { readonly status: "created"; readonly record: ComposerDraftImportRecord }
  | { readonly status: "existing"; readonly record: ComposerDraftImportRecord }
  | { readonly status: "idempotency-conflict" };

export type CommitComposerDraftImportResult =
  | { readonly status: "ready"; readonly record: ComposerDraftImportRecord }
  | { readonly status: "not-found" | "not-uploading" | "attachments-incomplete" };

export type ClaimComposerDraftImportResult =
  | { readonly status: "claimed"; readonly record: ComposerDraftImportRecord }
  | {
      readonly status:
        | "not-found"
        | "not-ready"
        | "lease-active"
        | "terminal"
        | "expired";
    };

export type CompleteComposerDraftImportResult =
  | { readonly status: "completed"; readonly record: ComposerDraftImportRecord }
  | {
      readonly status:
        | "not-found"
        | "lease-mismatch"
        | "verification-mismatch"
        | "not-claiming";
    };

export type CancelComposerDraftImportResult =
  | { readonly status: "cancelled" | "completed"; readonly record: ComposerDraftImportRecord }
  | { readonly status: "not-found" };

export interface ComposerDraftImportRepositoryShape {
  readonly create: (input: {
    readonly importId: string;
    readonly idempotencyKey: string;
    readonly payloadFingerprint: string;
    readonly source: ComposerDraftImportSource;
    readonly promptKind: ComposerDraftImportPromptKind;
    readonly projectId: string;
    readonly draftThreadId: string;
    readonly title: string;
    readonly prompt: string;
    readonly promptHash: string;
    readonly presets: ComposerDraftImportPresets;
    readonly attachments: ReadonlyArray<ComposerDraftImportAttachmentManifest>;
    readonly expiresAt: string;
    readonly now: string;
  }) => Effect.Effect<CreateComposerDraftImportResult, PersistenceSqlError>;
  readonly findById: (
    importId: string,
  ) => Effect.Effect<Option.Option<ComposerDraftImportRecord>, PersistenceSqlError>;
  readonly listAttachments: (
    importId: string,
  ) => Effect.Effect<ReadonlyArray<ComposerDraftImportAttachmentRecord>, PersistenceSqlError>;
  readonly attachManagedBlob: (input: {
    readonly importId: string;
    readonly attachmentId: string;
    readonly managedAttachmentId: string;
    readonly now: string;
  }) => Effect.Effect<boolean, PersistenceSqlError>;
  readonly commit: (input: {
    readonly importId: string;
    readonly now: string;
  }) => Effect.Effect<CommitComposerDraftImportResult, PersistenceSqlError>;
  readonly claim: (input: {
    readonly importId: string;
    readonly leaseId: string;
    readonly leaseExpiresAt: string;
    readonly now: string;
  }) => Effect.Effect<ClaimComposerDraftImportResult, PersistenceSqlError>;
  readonly complete: (input: {
    readonly importId: string;
    readonly leaseId: string;
    readonly promptHash: string;
    readonly attachmentIds: ReadonlyArray<string>;
    readonly now: string;
  }) => Effect.Effect<CompleteComposerDraftImportResult, PersistenceSqlError>;
  readonly cancel: (input: {
    readonly importId: string;
    readonly now: string;
  }) => Effect.Effect<CancelComposerDraftImportResult, PersistenceSqlError>;
  readonly recover: (input: {
    readonly now: string;
  }) => Effect.Effect<ReadonlyArray<ComposerDraftImportRecord>, PersistenceSqlError>;
}

export class ComposerDraftImportRepository extends ServiceMap.Service<
  ComposerDraftImportRepository,
  ComposerDraftImportRepositoryShape
>()("synara/persistence/Services/ComposerDraftImports/ComposerDraftImportRepository") {}

