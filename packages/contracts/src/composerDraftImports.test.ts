import { describe, expect, it } from "vitest";
import { Schema } from "effect";

import {
  ComposerDraftImportCompleteInput,
  ComposerDraftImportCreateInput,
  ComposerDraftImportId,
} from "./composerDraftImports";

const sha = "a".repeat(64);

const representativeCreate = {
  idempotencyKey: `pragma:prompt:${sha}:attempt`,
  source: {
    app: "pragma",
    promptId: "prompt-1",
    revisionId: "revision-1",
    fingerprint: sha,
  },
  promptKind: "oneOff",
  projectId: "project-1",
  title: "Prepare release",
  prompt: "Prepare the exact release handoff.",
  promptHash: sha,
  attachments: [
    {
      id: "screen-1",
      kind: "image",
      name: "screen.png",
      mimeType: "image/png",
      sizeBytes: 42,
      order: 0,
    },
  ],
  presets: {
    modelSelection: {
      instanceId: "codex-main",
      model: "gpt-5.6",
      options: [{ id: "reasoning", value: "high" }],
    },
    skills: [{ name: "release", path: "/skills/release/SKILL.md" }],
    interactionMode: "plan",
    environment: "worktree",
  },
} as const;

describe("composer draft import contracts", () => {
  it("decodes the Pragma create payload without replacing shared preset wire types", () => {
    const decoded = Schema.decodeUnknownSync(ComposerDraftImportCreateInput)(
      representativeCreate,
    );
    expect(decoded.projectId).toBe("project-1");
    expect(decoded.attachments[0]).toMatchObject({ id: "screen-1", order: 0 });
    expect(decoded.presets.modelSelection).toMatchObject({
      instanceId: "codex-main",
      model: "gpt-5.6",
    });
  });

  it("rejects malformed opaque IDs and oversized attachment sets", () => {
    expect(() => Schema.decodeUnknownSync(ComposerDraftImportId)("../../../token")).toThrow();
    expect(() =>
      Schema.decodeUnknownSync(ComposerDraftImportCreateInput)({
        ...representativeCreate,
        attachments: Array.from({ length: 9 }, (_, index) => ({
          id: `file-${index}`,
          kind: "file",
          name: `file-${index}.txt`,
          mimeType: "text/plain",
          sizeBytes: 1,
          order: index,
        })),
      }),
    ).toThrow();
  });

  it("requires complete verification to carry the lease and exact identities", () => {
    expect(
      Schema.decodeUnknownSync(ComposerDraftImportCompleteInput)({
        importId: "cdi_abc123",
        leaseId: "cdl_def456",
        promptHash: sha,
        attachmentIds: ["screen-1"],
      }),
    ).toMatchObject({
      importId: "cdi_abc123",
      leaseId: "cdl_def456",
      attachmentIds: ["screen-1"],
    });
  });
});

