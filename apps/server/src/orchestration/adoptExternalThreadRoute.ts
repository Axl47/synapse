import {
  CommandId,
  type OrchestrationAdoptExternalThreadInput,
  type OrchestrationAdoptExternalThreadResult,
  ThreadId,
} from "@synara/contracts";
import { getDefaultModel } from "@synara/shared/model";
import {
  providerStartOptionsFromInstance,
  resolveProviderInstance,
} from "@synara/shared/providerInstances";
import type { FileSystem, Path } from "effect";
import { Data, Effect } from "effect";

import type { ProviderAdapterRegistryShape } from "../provider/Services/ProviderAdapterRegistry";
import type { ProviderSessionDirectoryShape } from "../provider/Services/ProviderSessionDirectory";
import type { ServerSettingsShape } from "../serverSettings";
import type { OrchestrationEngineShape } from "./Services/OrchestrationEngine";
import type { ProjectionSnapshotQueryShape } from "./Services/ProjectionSnapshotQuery";
import { resolveExternalThreadWorkspaceMatch } from "./externalThreadWorkspace";
import type { makeImportThreadHandler } from "./importThreadRoute";

class AdoptExternalThreadError extends Data.TaggedError("AdoptExternalThreadError")<{
  readonly message: string;
}> {}

function adoptError(message: string): AdoptExternalThreadError {
  return new AdoptExternalThreadError({ message });
}

function externalThreadIdFromResumeCursor(value: unknown): string | null {
  if (!value || typeof value !== "object") return null;
  const cursor = value as Record<string, unknown>;
  return typeof cursor.threadId === "string" && cursor.threadId.trim().length > 0
    ? cursor.threadId.trim()
    : null;
}

function adoptionTitle(input: {
  readonly name?: string | null;
  readonly preview?: string | null;
  readonly externalThreadId: string;
}): string {
  const title = input.name?.trim() || input.preview?.trim();
  if (title) return title.slice(0, 240);
  const suffix = input.externalThreadId.slice(-8);
  return `Imported Codex thread${suffix ? ` ${suffix}` : ""}`;
}

export interface AdoptExternalThreadHandlerOptions {
  readonly fileSystem: FileSystem.FileSystem;
  readonly importThread: ReturnType<typeof makeImportThreadHandler>;
  readonly orchestrationEngine: OrchestrationEngineShape;
  readonly path: Path.Path;
  readonly platform: NodeJS.Platform;
  readonly projectionSnapshotQuery: ProjectionSnapshotQueryShape;
  readonly providerAdapterRegistry: ProviderAdapterRegistryShape;
  readonly providerSessionDirectory: ProviderSessionDirectoryShape;
  readonly serverSettings: ServerSettingsShape;
}

export function makeAdoptExternalThreadHandler(options: AdoptExternalThreadHandlerOptions) {
  const inFlight = new Map<string, Promise<OrchestrationAdoptExternalThreadResult>>();

  const adopt = Effect.fnUntraced(function* (input: OrchestrationAdoptExternalThreadInput) {
    const externalThreadId = input.externalThreadId.trim();
    const existingBindings = yield* options.providerSessionDirectory.listBindings();
    const existing = existingBindings.find(
      (binding) =>
        binding.provider === "codex" &&
        binding.providerInstanceId === input.providerInstanceId &&
        externalThreadIdFromResumeCursor(binding.resumeCursor) === externalThreadId,
    );
    if (existing) {
      return { threadId: existing.threadId } satisfies OrchestrationAdoptExternalThreadResult;
    }

    const settings = yield* options.serverSettings.getSettings;
    const instance = resolveProviderInstance(settings, {
      instanceId: input.providerInstanceId,
      provider: "codex",
    });
    if (!instance || !instance.enabled || instance.driver !== "codex") {
      return yield* adoptError(
        `Codex provider instance '${input.providerInstanceId}' is unavailable.`,
      );
    }

    const shell = yield* options.projectionSnapshotQuery.getShellSnapshot();
    const project = shell.projects.find((candidate) => candidate.id === input.projectId);
    if (!project) {
      return yield* adoptError(`Project '${input.projectId}' was not found.`);
    }

    const adapter = options.providerAdapterRegistry.getByInstance
      ? yield* options.providerAdapterRegistry.getByInstance(instance.instanceId)
      : yield* options.providerAdapterRegistry.getByProvider("codex");
    if (!adapter.readExternalThread) {
      return yield* adoptError("The installed Codex provider cannot read external threads.");
    }
    const providerOptions = providerStartOptionsFromInstance(instance);
    const snapshot = yield* adapter
      .readExternalThread({
        externalThreadId,
        providerInstanceId: instance.instanceId,
        ...(providerOptions ? { providerOptions } : {}),
      })
      .pipe(
        Effect.mapError((cause) =>
          adoptError(
            cause instanceof Error && cause.message.trim().length > 0
              ? cause.message.trim()
              : "The Codex thread could not be read.",
          ),
        ),
      );
    const cwd = snapshot.cwd?.trim();
    if (!cwd) {
      return yield* adoptError("The Codex thread does not include a working directory.");
    }
    const match = yield* resolveExternalThreadWorkspaceMatch({
      cwd,
      fileSystem: options.fileSystem,
      path: options.path,
      platform: options.platform,
      projects: shell.projects,
    });
    const explicitlyAssignedUnmatchedProject =
      match.projectId === null && input.allowUnmatchedProject === true;
    if (match.projectId !== project.id && !explicitlyAssignedUnmatchedProject) {
      return yield* adoptError(
        match.projectId === null
          ? "The Codex thread is not safely associated with this project. Add its folder as a project first."
          : "The Codex thread belongs to a different project.",
      );
    }

    const projectModel = project.defaultModelSelection;
    const modelSelection =
      projectModel?.instanceId === instance.instanceId
        ? projectModel
        : { instanceId: instance.instanceId, model: getDefaultModel("codex") };
    const threadId = ThreadId.makeUnsafe(crypto.randomUUID());
    const createdAt = new Date().toISOString();
    let created = false;
    yield* options.orchestrationEngine
      .dispatch({
        type: "thread.create",
        commandId: CommandId.makeUnsafe(crypto.randomUUID()),
        threadId,
        projectId: project.id,
        title: adoptionTitle({
          externalThreadId,
          ...(snapshot.name !== undefined ? { name: snapshot.name } : {}),
          ...(snapshot.preview !== undefined ? { preview: snapshot.preview } : {}),
        }),
        modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        envMode: settings.defaultThreadEnvMode,
        branch: null,
        worktreePath: null,
        createdAt,
      })
      .pipe(Effect.tap(() => Effect.sync(() => (created = true))));

    const imported = yield* options.importThread({ threadId, externalId: externalThreadId }).pipe(
      Effect.catch((cause) =>
        created
          ? options.orchestrationEngine
              .dispatch({
                type: "thread.delete",
                commandId: CommandId.makeUnsafe(crypto.randomUUID()),
                threadId,
              })
              .pipe(
                Effect.catch(() => Effect.void),
                Effect.flatMap(() => Effect.fail(cause)),
              )
          : Effect.fail(cause),
      ),
    );
    return { threadId: imported.threadId } satisfies OrchestrationAdoptExternalThreadResult;
  });

  return (input: OrchestrationAdoptExternalThreadInput) => {
    const key = `${input.providerInstanceId}:${input.externalThreadId.trim()}`;
    return Effect.tryPromise({
      try: () => {
        const existing = inFlight.get(key);
        if (existing) return existing;
        const promise = Effect.runPromise(adopt(input)).finally(() => {
          if (inFlight.get(key) === promise) inFlight.delete(key);
        });
        inFlight.set(key, promise);
        return promise;
      },
      catch: (cause) =>
        cause instanceof AdoptExternalThreadError
          ? cause
          : adoptError(
              cause instanceof Error && cause.message.trim().length > 0
                ? cause.message.trim()
                : "Failed to adopt external Codex thread.",
            ),
    });
  };
}
