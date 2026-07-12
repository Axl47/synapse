import path from "node:path";

import {
  DEFAULT_SERVER_SETTINGS,
  ProjectId,
  ThreadId,
  type ProviderInstanceId,
} from "@synara/contracts";
import type { FileSystem, Path } from "effect";
import { Effect } from "effect";
import { describe, expect, it, vi } from "vitest";

import { makeAdoptExternalThreadHandler } from "./adoptExternalThreadRoute";

const noFiles = {
  readFileString: () => Effect.fail(new Error("missing")),
} as unknown as FileSystem.FileSystem;

function baseOptions(overrides: Record<string, unknown> = {}) {
  const projectId = ProjectId.makeUnsafe("project-1");
  return {
    fileSystem: noFiles,
    path: path as unknown as Path.Path,
    platform: "darwin" as const,
    projectionSnapshotQuery: {
      getShellSnapshot: () =>
        Effect.succeed({
          snapshotSequence: 1,
          projects: [
            {
              id: projectId,
              workspaceRoot: "/repo",
              defaultModelSelection: null,
            } as never,
          ],
          threads: [],
        }),
    } as never,
    providerAdapterRegistry: {
      getByInstance: () =>
        Effect.succeed({
          readExternalThread: () =>
            Effect.succeed({
              threadId: ThreadId.makeUnsafe("external-1"),
              cwd: "/repo",
              name: "External task",
              turns: [],
            }),
        } as never),
      getByProvider: () => Effect.die("not used"),
      listProviders: () => Effect.succeed([]),
    },
    providerSessionDirectory: {
      listBindings: () => Effect.succeed([]),
    } as never,
    serverSettings: {
      getSettings: Effect.succeed(DEFAULT_SERVER_SETTINGS),
    } as never,
    ...overrides,
  };
}

describe("makeAdoptExternalThreadHandler", () => {
  it("returns an existing binding without creating another thread", async () => {
    const dispatch = vi.fn(() => Effect.void);
    const importThread = vi.fn(() => Effect.die("not used"));
    const handler = makeAdoptExternalThreadHandler({
      ...baseOptions({
        orchestrationEngine: { dispatch } as never,
        importThread: importThread as never,
        providerSessionDirectory: {
          listBindings: () =>
            Effect.succeed([
              {
                threadId: ThreadId.makeUnsafe("local-1"),
                provider: "codex",
                providerInstanceId: "codex" as ProviderInstanceId,
                resumeCursor: { threadId: "external-1" },
              },
            ]),
        } as never,
      }),
    });

    await expect(
      Effect.runPromise(
        handler({
          providerInstanceId: "codex" as ProviderInstanceId,
          externalThreadId: "external-1",
          projectId: ProjectId.makeUnsafe("project-1"),
        }),
      ),
    ).resolves.toEqual({ threadId: "local-1" });
    expect(dispatch).not.toHaveBeenCalled();
    expect(importThread).not.toHaveBeenCalled();
  });

  it("serializes concurrent adoption and reuses the existing import path", async () => {
    const commands: Array<Record<string, unknown>> = [];
    let importedThreadId: ThreadId | null = null;
    const handler = makeAdoptExternalThreadHandler({
      ...baseOptions({
        orchestrationEngine: {
          dispatch: (command: Record<string, unknown>) => {
            commands.push(command);
            return Effect.succeed({ sequence: commands.length });
          },
        } as never,
        importThread: ((input: { threadId: ThreadId }) => {
          importedThreadId = input.threadId;
          return Effect.promise(
            () =>
              new Promise<{ threadId: ThreadId }>((resolve) =>
                setTimeout(() => resolve({ threadId: input.threadId }), 5),
              ),
          );
        }) as never,
      }),
    });
    const input = {
      providerInstanceId: "codex" as ProviderInstanceId,
      externalThreadId: "external-1",
      projectId: ProjectId.makeUnsafe("project-1"),
    };

    const [first, second] = await Promise.all([
      Effect.runPromise(handler(input)),
      Effect.runPromise(handler(input)),
    ]);

    expect(first).toEqual(second);
    expect(first.threadId).toBe(importedThreadId);
    expect(commands).toHaveLength(1);
    expect(commands[0]).toMatchObject({
      type: "thread.create",
      projectId: "project-1",
      title: "External task",
      modelSelection: { instanceId: "codex", model: "gpt-5.6-sol" },
    });
  });

  it("deletes the local thread when import fails", async () => {
    const commands: Array<Record<string, unknown>> = [];
    const handler = makeAdoptExternalThreadHandler({
      ...baseOptions({
        orchestrationEngine: {
          dispatch: (command: Record<string, unknown>) => {
            commands.push(command);
            return Effect.succeed({ sequence: commands.length });
          },
        } as never,
        importThread: (() => Effect.fail(new Error("resume failed"))) as never,
      }),
    });

    await expect(
      Effect.runPromise(
        handler({
          providerInstanceId: "codex" as ProviderInstanceId,
          externalThreadId: "external-1",
          projectId: ProjectId.makeUnsafe("project-1"),
        }),
      ),
    ).rejects.toThrow("resume failed");
    expect(commands.map((command) => command.type)).toEqual(["thread.create", "thread.delete"]);
  });

  it("rejects adoption into a different project", async () => {
    const handler = makeAdoptExternalThreadHandler({
      ...baseOptions({
        orchestrationEngine: { dispatch: () => Effect.void } as never,
        importThread: (() => Effect.die("not used")) as never,
      }),
    });

    await expect(
      Effect.runPromise(
        handler({
          providerInstanceId: "codex" as ProviderInstanceId,
          externalThreadId: "external-1",
          projectId: ProjectId.makeUnsafe("other-project"),
        }),
      ),
    ).rejects.toThrow("was not found");
  });
});
