import {
  type ComposerDraftImportClaimResult,
  type ComposerDraftImportId,
  type ModelSelection,
  type NativeApi,
  type ProjectId,
  type ThreadId,
} from "@synara/contracts";
import { inferLegacyProviderKindFromModelSelection } from "@synara/shared/providerInstances";

import {
  type ComposerFileAttachment,
  type ComposerImageAttachment,
  useComposerDraftStore,
} from "./composerDraftStore";
import {
  persistComposerFileBlob,
  persistComposerImageBlob,
  readComposerFileBlob,
  readComposerImageBlob,
} from "./lib/composerImageBlobStore";
import { resolveWsHttpUrl } from "./lib/wsHttpUrl";

export class ExternalComposerDraftImportError extends Error {
  readonly code: string;

  constructor(message: string, code: string, options?: { readonly cause?: unknown }) {
    super(message, options?.cause === undefined ? undefined : { cause: options.cause });
    this.name = "ExternalComposerDraftImportError";
    this.code = code;
  }
}

export interface ExternalComposerDraftBlobStore {
  readonly persistImage: typeof persistComposerImageBlob;
  readonly persistFile: typeof persistComposerFileBlob;
  readonly readImage: typeof readComposerImageBlob;
  readonly readFile: typeof readComposerFileBlob;
}

const browserBlobStore: ExternalComposerDraftBlobStore = {
  persistImage: persistComposerImageBlob,
  persistFile: persistComposerFileBlob,
  readImage: readComposerImageBlob,
  readFile: readComposerFileBlob,
};

async function sha256(bytes: Uint8Array | string): Promise<string> {
  const input = typeof bytes === "string" ? new TextEncoder().encode(bytes) : bytes;
  const digest = await crypto.subtle.digest("SHA-256", input);
  return Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, "0")).join("");
}

function validateModelSelection(
  selection: ModelSelection,
  models: Awaited<ReturnType<NativeApi["provider"]["listModels"]>>["models"],
): void {
  const model = models.find((candidate) => candidate.slug === selection.model);
  if (!model) {
    throw new ExternalComposerDraftImportError(
      `The selected model '${selection.model}' is no longer available.`,
      "MODEL_PRESET_UNAVAILABLE",
    );
  }
  for (const selectedOption of selection.options ?? []) {
    const descriptor = model.optionDescriptors?.find(
      (candidate) => candidate.id === selectedOption.id,
    );
    const valid =
      descriptor?.type === "boolean"
        ? typeof selectedOption.value === "boolean"
        : descriptor?.type === "select"
          ? typeof selectedOption.value === "string" &&
            descriptor.options.some((option) => option.id === selectedOption.value)
          : false;
    if (!valid) {
      throw new ExternalComposerDraftImportError(
        `The selected model option '${selectedOption.id}' is no longer available.`,
        "MODEL_OPTION_PRESET_UNAVAILABLE",
      );
    }
  }
}

async function validatePresets(input: {
  readonly claim: ComposerDraftImportClaimResult;
  readonly api: NativeApi;
  readonly workspaceRoot: string;
}): Promise<void> {
  const { presets } = input.claim;
  await Promise.all([
    presets.modelSelection
      ? input.api.provider
          .listModels({
            provider: inferLegacyProviderKindFromModelSelection(presets.modelSelection),
            instanceId: presets.modelSelection.instanceId,
            cwd: input.workspaceRoot,
          })
          .then((result) => validateModelSelection(presets.modelSelection!, result.models))
      : Promise.resolve(),
    presets.skills.length > 0
      ? input.api.provider.listSkillsCatalog({ cwd: input.workspaceRoot }).then((result) => {
          for (const expected of presets.skills) {
            if (
              !result.skills.some(
                (skill) =>
                  skill.enabled && skill.name === expected.name && skill.path === expected.path,
              )
            ) {
              throw new ExternalComposerDraftImportError(
                `The selected skill '${expected.name}' is no longer available at its saved path.`,
                "SKILL_PRESET_UNAVAILABLE",
              );
            }
          }
        })
      : Promise.resolve(),
  ]);
}

async function downloadClaimAttachment(
  attachment: ComposerDraftImportClaimResult["attachments"][number],
  fetchImpl: typeof fetch,
): Promise<{
  readonly attachment: ComposerDraftImportClaimResult["attachments"][number];
  readonly file: File;
}> {
  const response = await fetchImpl(resolveWsHttpUrl(attachment.downloadPath), {
    method: "GET",
    credentials: "include",
    cache: "no-store",
  });
  if (!response.ok) {
    throw new ExternalComposerDraftImportError(
      `Could not download '${attachment.name}'.`,
      "ATTACHMENT_DOWNLOAD_FAILED",
    );
  }
  const responseType = response.headers.get("content-type")?.split(";", 1)[0]?.toLowerCase();
  if (responseType !== attachment.mimeType.toLowerCase()) {
    throw new ExternalComposerDraftImportError(
      `The downloaded type for '${attachment.name}' did not match its declaration.`,
      "ATTACHMENT_TYPE_MISMATCH",
    );
  }
  const bytes = new Uint8Array(await response.arrayBuffer());
  if (
    bytes.byteLength !== attachment.sizeBytes ||
    (await sha256(bytes)) !== attachment.sha256
  ) {
    throw new ExternalComposerDraftImportError(
      `The downloaded bytes for '${attachment.name}' did not match the import.`,
      "ATTACHMENT_VERIFICATION_FAILED",
    );
  }
  return {
    attachment,
    file: new File([bytes], attachment.name, {
      type: attachment.mimeType,
      lastModified: Date.now(),
    }),
  };
}

async function verifyPersistedDraft(input: {
  readonly threadId: string;
  readonly promptHash: string;
  readonly attachmentIds: ReadonlyArray<string>;
}): void {
  if (typeof localStorage === "undefined") {
    throw new ExternalComposerDraftImportError(
      "Composer draft storage is unavailable.",
      "DRAFT_STORAGE_UNAVAILABLE",
    );
  }
  const root = JSON.parse(localStorage.getItem("synara:composer-drafts:v1") ?? "null");
  const draft = root?.state?.draftsByThreadId?.[input.threadId];
  const persistedIds = [
    ...(Array.isArray(draft?.attachments)
      ? draft.attachments.map((attachment: { readonly id?: unknown }) => attachment.id)
      : []),
    ...(Array.isArray(draft?.files)
      ? draft.files.map((attachment: { readonly id?: unknown }) => attachment.id)
      : []),
  ];
  if (
    typeof draft?.prompt !== "string" ||
    (await sha256(draft.prompt)) !== input.promptHash ||
    persistedIds.length !== input.attachmentIds.length ||
    input.attachmentIds.some((id) => !persistedIds.includes(id))
  ) {
    throw new ExternalComposerDraftImportError(
      "The persisted composer draft did not match the imported package.",
      "DRAFT_VERIFICATION_FAILED",
    );
  }
}

export async function hydrateExternalComposerDraftImport(input: {
  readonly importId: ComposerDraftImportId;
  readonly api: NativeApi;
  readonly projectWorkspaceRoot: (projectId: ProjectId) => string | null;
  readonly fetchImpl?: typeof fetch;
  readonly blobStore?: ExternalComposerDraftBlobStore;
}): Promise<{ readonly threadId: ThreadId }> {
  const claim = await input.api.composerDraftImports.claim({ importId: input.importId });
  const workspaceRoot = input.projectWorkspaceRoot(claim.projectId);
  if (!workspaceRoot) {
    throw new ExternalComposerDraftImportError(
      "The imported draft's project is no longer available.",
      "PROJECT_UNAVAILABLE",
    );
  }
  await validatePresets({ claim, api: input.api, workspaceRoot });

  const store = useComposerDraftStore.getState();
  if (
    store.getDraftThread(claim.draftThreadId) ||
    store.draftsByThreadId[claim.draftThreadId]
  ) {
    throw new ExternalComposerDraftImportError(
      "The imported draft thread already exists.",
      "DRAFT_THREAD_COLLISION",
    );
  }

  const downloaded = await Promise.all(
    claim.attachments.map((attachment) =>
      downloadClaimAttachment(attachment, input.fetchImpl ?? fetch),
    ),
  );
  const previewUrls: string[] = [];
  const blobStore = input.blobStore ?? browserBlobStore;
  let registered = false;
  try {
    const images: ComposerImageAttachment[] = [];
    const files: ComposerFileAttachment[] = [];
    for (const entry of downloaded) {
      if (entry.attachment.kind === "image") {
        const previewUrl = URL.createObjectURL(entry.file);
        previewUrls.push(previewUrl);
        images.push({
          type: "image",
          id: entry.attachment.id,
          name: entry.attachment.name,
          mimeType: entry.attachment.mimeType,
          sizeBytes: entry.attachment.sizeBytes,
          previewUrl,
          file: entry.file,
        });
      } else {
        files.push({
          type: "file",
          id: entry.attachment.id,
          name: entry.attachment.name,
          mimeType: entry.attachment.mimeType,
          sizeBytes: entry.attachment.sizeBytes,
          file: entry.file,
        });
      }
    }

    store.registerDraftThread(claim.draftThreadId, {
      projectId: claim.projectId,
      ...(claim.presets.environment ? { envMode: claim.presets.environment } : {}),
      ...(claim.presets.interactionMode
        ? { interactionMode: claim.presets.interactionMode }
        : {}),
      entryPoint: "chat",
    });
    registered = true;
    store.setPrompt(claim.draftThreadId, claim.prompt);
    store.addImages(claim.draftThreadId, images);
    store.addFiles(claim.draftThreadId, files);
    store.setSkills(claim.draftThreadId, [...claim.presets.skills]);
    if (claim.presets.modelSelection) {
      store.setModelSelection(claim.draftThreadId, claim.presets.modelSelection);
    }
    if (claim.presets.interactionMode) {
      store.setInteractionMode(claim.draftThreadId, claim.presets.interactionMode);
    }

    const persistedImages = await Promise.all(
      images.map(async (image) => ({
        id: image.id,
        name: image.name,
        mimeType: image.mimeType,
        sizeBytes: image.sizeBytes,
        blobKey: await blobStore.persistImage({
          threadId: claim.draftThreadId,
          imageId: image.id,
          file: image.file,
        }),
      })),
    );
    const persistedFiles = await Promise.all(
      files.map(async (file) => ({
        id: file.id,
        name: file.name,
        mimeType: file.mimeType,
        sizeBytes: file.sizeBytes,
        blobKey: await blobStore.persistFile({
          threadId: claim.draftThreadId,
          fileId: file.id,
          file: file.file,
        }),
      })),
    );
    const imagePersistence =
      persistedImages.length === 0
        ? "persisted"
        : await store.syncPersistedAttachments(claim.draftThreadId, persistedImages);
    // Always run the file sync, even for an image-only or text-only import. Its
    // verified flush commits the complete draft after the image state settles.
    const filePersistence = await store.syncPersistedFiles(
      claim.draftThreadId,
      persistedFiles,
    );
    if (imagePersistence !== "persisted" || filePersistence !== "persisted") {
      throw new ExternalComposerDraftImportError(
        `The imported attachments could not be durably saved (images: ${imagePersistence}, files: ${filePersistence}).`,
        "ATTACHMENT_PERSISTENCE_FAILED",
      );
    }

    await verifyPersistedDraft({
      threadId: claim.draftThreadId,
      promptHash: claim.promptHash,
      attachmentIds: claim.attachments.map((attachment) => attachment.id),
    });
    for (const persisted of [...persistedImages, ...persistedFiles]) {
      const expected = claim.attachments.find((attachment) => attachment.id === persisted.id);
      const file =
        expected?.kind === "image"
          ? await blobStore.readImage(persisted.blobKey)
          : await blobStore.readFile(persisted.blobKey);
      if (
        !file ||
        !expected ||
        (await sha256(new Uint8Array(await file.arrayBuffer()))) !== expected.sha256
      ) {
        throw new ExternalComposerDraftImportError(
          `The persisted bytes for '${persisted.name}' could not be verified.`,
          "ATTACHMENT_PERSISTENCE_FAILED",
        );
      }
    }

    await input.api.composerDraftImports.complete({
      importId: claim.importId,
      leaseId: claim.leaseId,
      promptHash: claim.promptHash,
      attachmentIds: claim.attachments.map((attachment) => attachment.id),
    });
    return { threadId: claim.draftThreadId };
  } catch (cause) {
    if (registered) {
      useComposerDraftStore.getState().clearDraftThread(claim.draftThreadId);
    }
    for (const previewUrl of previewUrls) URL.revokeObjectURL(previewUrl);
    throw cause;
  }
}
