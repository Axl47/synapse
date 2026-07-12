import path from "node:path";

import { DEFAULT_SERVER_SETTINGS, ProjectId, type ProviderInstanceId } from "@synara/contracts";
import type { FileSystem, Path } from "effect";
import { Effect } from "effect";
import { describe, expect, it, vi } from "vitest";

import { makeExternalThreadDiscoveryHandler } from "./externalThreadDiscoveryRoute";

const noFiles = {
  readFileString: () => Effect.fail(new Error("missing")),
} as unknown as FileSystem.FileSystem;

function summary(externalThreadId: string, cwd: string, updatedAtSeconds: number) {
  return {
    externalThreadId,
    name: null,
    preview: `Task ${externalThreadId}`,
    cwd,
    sourceKind: "appServer",
    status: "idle" as const,
    modelProvider: "openai",
    createdAtSeconds: updatedAtSeconds - 10,
    updatedAtSeconds,
    recencyAtSeconds: null,
  };
}

describe("makeExternalThreadDiscoveryHandler", () => {
  it("discovers across accounts, deduplicates bindings, and preserves partial failures", async () => {
    const listDefault = vi.fn(() =>
      Effect.succeed({
        threads: [summary("bound", "/repo", 100), summary("fresh", "/repo/src", 200)],
        truncated: false,
      }),
    );
    const listWork = vi.fn(() => Effect.fail(new Error("work account unavailable")));
    const handler = makeExternalThreadDiscoveryHandler({
      fileSystem: noFiles,
      path: path as unknown as Path.Path,
      platform: "darwin",
      projectionSnapshotQuery: {
        getShellSnapshot: () =>
          Effect.succeed({
            snapshotSequence: 1,
            projects: [
              {
                id: ProjectId.makeUnsafe("project-1"),
                workspaceRoot: "/repo",
              } as never,
            ],
            threads: [],
          }),
      } as never,
      providerAdapterRegistry: {
        getByInstance: (instanceId: ProviderInstanceId) =>
          Effect.succeed({
            listExternalThreads: instanceId === "codex" ? listDefault : listWork,
          } as never),
        getByProvider: () => Effect.die("not used"),
        listProviders: () => Effect.succeed([]),
      },
      providerSessionDirectory: {
        listBindings: () =>
          Effect.succeed([
            {
              threadId: "local-thread",
              provider: "codex",
              providerInstanceId: "codex",
              resumeCursor: { threadId: "bound" },
            } as never,
          ]),
      } as never,
      serverSettings: {
        getSettings: Effect.succeed({
          ...DEFAULT_SERVER_SETTINGS,
          providerInstances: {
            codex_work: {
              driver: "codex",
              enabled: true,
              config: { accountId: "work" },
            },
          },
        }),
      } as never,
    });

    const result = await Effect.runPromise(handler({ refresh: true }));

    expect(result.candidates).toHaveLength(1);
    expect(result.candidates[0]).toMatchObject({
      externalThreadId: "fresh",
      matchedProjectId: "project-1",
      matchKind: "descendant",
      providerInstanceId: "codex",
    });
    expect(result.warnings).toEqual([
      { providerInstanceId: "codex_work", message: "work account unavailable" },
    ]);
    expect(listDefault).toHaveBeenCalledWith(
      expect.objectContaining({ useStateDbOnly: false, maxThreads: 200 }),
    );
  });

  it("reuses the short instance cache unless refresh is requested", async () => {
    const listExternalThreads = vi.fn(() =>
      Effect.succeed({ threads: [summary("fresh", "/repo", 100)], truncated: true }),
    );
    const handler = makeExternalThreadDiscoveryHandler({
      fileSystem: noFiles,
      path: path as unknown as Path.Path,
      platform: "darwin",
      projectionSnapshotQuery: {
        getShellSnapshot: () => Effect.succeed({ snapshotSequence: 1, projects: [], threads: [] }),
      } as never,
      providerAdapterRegistry: {
        getByInstance: () => Effect.succeed({ listExternalThreads } as never),
        getByProvider: () => Effect.die("not used"),
        listProviders: () => Effect.succeed([]),
      },
      providerSessionDirectory: { listBindings: () => Effect.succeed([]) } as never,
      serverSettings: { getSettings: Effect.succeed(DEFAULT_SERVER_SETTINGS) } as never,
    });

    const first = await Effect.runPromise(handler({}));
    const second = await Effect.runPromise(handler({}));

    expect(first.truncated).toBe(true);
    expect(second.candidates).toEqual(first.candidates);
    expect(listExternalThreads).toHaveBeenCalledTimes(1);
  });
});
