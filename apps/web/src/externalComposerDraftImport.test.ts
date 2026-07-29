import {
  ComposerDraftImportId,
  type NativeApi,
  ProjectId,
  ThreadId,
} from "@synara/contracts";
import { beforeEach, describe, expect, it, vi } from "vitest";

import {
  type ExternalComposerDraftBlobStore,
  hydrateExternalComposerDraftImport,
} from "./externalComposerDraftImport";
import { useComposerDraftStore } from "./composerDraftStore";
import { resetComposerDraftStore } from "./composerDraftStoreTestFixtures";

const importId = ComposerDraftImportId.makeUnsafe("cdi_importtest");
const projectId = ProjectId.makeUnsafe("project-1");
const threadId = ThreadId.makeUnsafe("draft-import-1");
const prompt = "Imported prompt";
const promptHash = "2d73dc3e27909a4ba81fd26c28040440fe14a74e4adcc5938edbf15b9f448766";
const fileHash = "3b9c358f36f0a31b6ad3e14f309c7cf198ac9246e8316f9ce543d5b19ac02b80";

function makeApi() {
  const complete = vi.fn(async () => ({
    importId,
    draftThreadId: threadId,
    status: "completed" as const,
    updatedAt: "2026-07-29T00:00:02.000Z",
    expiresAt: "2026-07-30T00:00:00.000Z",
    completedAt: "2026-07-29T00:00:02.000Z",
  }));
  const dispatchCommand = vi.fn();
  const api = {
    provider: {
      listModels: vi.fn(),
      listSkillsCatalog: vi.fn(),
    },
    orchestration: { dispatchCommand },
    composerDraftImports: {
      claim: vi.fn(async () => ({
        importId,
        leaseId: "cdl_importtest",
        leaseExpiresAt: "2026-07-29T00:02:00.000Z",
        draftThreadId: threadId,
        projectId,
        title: "Imported prompt",
        promptKind: "reusable" as const,
        prompt,
        promptHash,
        attachments: [
          {
            id: "file-1",
            kind: "file" as const,
            name: "context.txt",
            mimeType: "text/plain",
            sizeBytes: 4,
            order: 0,
            sha256: fileHash,
            downloadPath: "/composer-draft-imports/cdi_importtest/attachments/file-1/content",
          },
        ],
        presets: { skills: [] },
      })),
      complete,
    },
  } as unknown as NativeApi;
  return { api, complete, dispatchCommand };
}

function makeBlobStore(): ExternalComposerDraftBlobStore {
  const files = new Map<string, File>();
  return {
    persistImage: vi.fn(async ({ threadId, imageId, file }) => {
      const key = `${threadId}:${imageId}`;
      files.set(key, file);
      return key;
    }),
    persistFile: vi.fn(async ({ threadId, fileId, file }) => {
      const key = `${threadId}:file:${fileId}`;
      files.set(key, file);
      return key;
    }),
    readImage: vi.fn(async (key) => files.get(key) ?? null),
    readFile: vi.fn(async (key) => files.get(key) ?? null),
  };
}

describe("hydrateExternalComposerDraftImport", () => {
  beforeEach(() => {
    localStorage.clear();
    resetComposerDraftStore();
  });

  it("persists and re-reads an unsent standalone draft before acknowledging completion", async () => {
    const { api, complete, dispatchCommand } = makeApi();

    const result = await hydrateExternalComposerDraftImport({
      importId,
      api,
      projectWorkspaceRoot: (candidate) => (candidate === projectId ? "/workspace/project" : null),
      fetchImpl: vi.fn(
        async () =>
          new Response(new TextEncoder().encode("file"), {
            status: 200,
            headers: { "Content-Type": "text/plain" },
          }),
      ) as typeof fetch,
      blobStore: makeBlobStore(),
    });

    expect(result.threadId).toBe(threadId);
    expect(complete).toHaveBeenCalledWith({
      importId,
      leaseId: "cdl_importtest",
      promptHash,
      attachmentIds: ["file-1"],
    });
    expect(dispatchCommand).not.toHaveBeenCalled();
    expect(useComposerDraftStore.getState().projectDraftThreadIdByProjectId).toEqual({});
    expect(useComposerDraftStore.getState().draftThreadsByThreadId[threadId]?.projectId).toBe(
      projectId,
    );
    expect(useComposerDraftStore.getState().draftsByThreadId[threadId]).toMatchObject({
      prompt,
      files: [{ id: "file-1", name: "context.txt" }],
      persistedFiles: [{ id: "file-1" }],
    });
  });
});
