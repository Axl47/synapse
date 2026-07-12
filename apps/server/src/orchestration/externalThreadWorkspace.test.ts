import path from "node:path";

import { ProjectId } from "@synara/contracts";
import type { FileSystem, Path } from "effect";
import { Effect } from "effect";
import { describe, expect, it } from "vitest";

import { resolveExternalThreadWorkspaceMatch } from "./externalThreadWorkspace";

const project = (id: string, workspaceRoot: string) =>
  ({ id: ProjectId.makeUnsafe(id), workspaceRoot }) as never;

const noFiles = {
  readFileString: () => Effect.fail(new Error("missing")),
} as unknown as FileSystem.FileSystem;

describe("resolveExternalThreadWorkspaceMatch", () => {
  it("prefers exact roots and the most specific containing project", async () => {
    const projects = [project("root", "/repo"), project("nested", "/repo/packages/app")];

    await expect(
      Effect.runPromise(
        resolveExternalThreadWorkspaceMatch({
          cwd: "/repo",
          fileSystem: noFiles,
          path: path as unknown as Path.Path,
          platform: "darwin",
          projects,
        }),
      ),
    ).resolves.toEqual({ projectId: "root", matchKind: "exact" });

    await expect(
      Effect.runPromise(
        resolveExternalThreadWorkspaceMatch({
          cwd: "/repo/packages/app/src",
          fileSystem: noFiles,
          path: path as unknown as Path.Path,
          platform: "darwin",
          projects,
        }),
      ),
    ).resolves.toEqual({ projectId: "nested", matchKind: "descendant" });
  });

  it("recognizes a managed worktree owner", async () => {
    const fileSystem = {
      readFileString: (candidate: string) =>
        candidate === "/worktrees/task/.git"
          ? Effect.succeed("gitdir: /repo/.git/worktrees/task\n")
          : Effect.fail(new Error("missing")),
    } as unknown as FileSystem.FileSystem;

    await expect(
      Effect.runPromise(
        resolveExternalThreadWorkspaceMatch({
          cwd: "/worktrees/task/src",
          fileSystem,
          path: path as unknown as Path.Path,
          platform: "darwin",
          projects: [project("root", "/repo")],
        }),
      ),
    ).resolves.toEqual({ projectId: "root", matchKind: "managed-worktree" });
  });

  it("fails closed for unrelated workspaces", async () => {
    await expect(
      Effect.runPromise(
        resolveExternalThreadWorkspaceMatch({
          cwd: "/unknown/repo",
          fileSystem: noFiles,
          path: path as unknown as Path.Path,
          platform: "darwin",
          projects: [project("root", "/repo")],
        }),
      ),
    ).resolves.toEqual({ projectId: null, matchKind: "unmatched" });
  });
});
