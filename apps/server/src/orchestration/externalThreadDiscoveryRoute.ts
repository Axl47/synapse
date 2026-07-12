import type {
  ExternalThreadCandidate,
  OrchestrationListExternalThreadsInput,
  OrchestrationListExternalThreadsResult,
  ProviderInstanceId,
} from "@synara/contracts";
import {
  deriveProviderInstances,
  providerStartOptionsFromInstance,
} from "@synara/shared/providerInstances";
import type { FileSystem, Path } from "effect";
import { Effect } from "effect";

import type { ProviderAdapterRegistryShape } from "../provider/Services/ProviderAdapterRegistry";
import type { ProviderListExternalThreadsResult } from "../provider/Services/ProviderAdapter";
import type { ProviderSessionDirectoryShape } from "../provider/Services/ProviderSessionDirectory";
import type { ServerSettingsShape } from "../serverSettings";
import type { ProjectionSnapshotQueryShape } from "./Services/ProjectionSnapshotQuery";
import { resolveExternalThreadWorkspaceMatch } from "./externalThreadWorkspace";

const DISCOVERY_CACHE_TTL_MS = 15_000;
const DISCOVERY_MAX_THREADS_PER_INSTANCE = 200;

interface CachedInstanceThreads {
  readonly expiresAt: number;
  readonly optionsKey: string;
  readonly result: ProviderListExternalThreadsResult;
}

function errorMessage(cause: unknown): string {
  return cause instanceof Error && cause.message.trim().length > 0
    ? cause.message.trim()
    : "External thread discovery failed.";
}

function externalThreadIdFromResumeCursor(value: unknown): string | null {
  if (!value || typeof value !== "object") return null;
  const cursor = value as Record<string, unknown>;
  return typeof cursor.threadId === "string" && cursor.threadId.trim().length > 0
    ? cursor.threadId.trim()
    : null;
}

function toIsoDateTime(seconds: number): string {
  return new Date(Math.max(0, seconds) * 1000).toISOString();
}

export interface ExternalThreadDiscoveryHandlerOptions {
  readonly fileSystem: FileSystem.FileSystem;
  readonly path: Path.Path;
  readonly platform: NodeJS.Platform;
  readonly projectionSnapshotQuery: ProjectionSnapshotQueryShape;
  readonly providerAdapterRegistry: ProviderAdapterRegistryShape;
  readonly providerSessionDirectory: ProviderSessionDirectoryShape;
  readonly serverSettings: ServerSettingsShape;
}

export function makeExternalThreadDiscoveryHandler(options: ExternalThreadDiscoveryHandlerOptions) {
  const cache = new Map<ProviderInstanceId, CachedInstanceThreads>();

  return Effect.fnUntraced(function* (input: OrchestrationListExternalThreadsInput) {
    const startedAt = Date.now();
    const [settings, shell, bindings] = yield* Effect.all([
      options.serverSettings.getSettings,
      options.projectionSnapshotQuery.getShellSnapshot(),
      options.providerSessionDirectory.listBindings(),
    ]);
    const instances = deriveProviderInstances(settings).filter(
      (instance) => instance.enabled && instance.driver === "codex",
    );
    const boundExternalKeys = new Set(
      bindings.flatMap((binding) => {
        const externalThreadId = externalThreadIdFromResumeCursor(binding.resumeCursor);
        return binding.provider === "codex" && externalThreadId
          ? [`${binding.providerInstanceId}:${externalThreadId}`]
          : [];
      }),
    );
    const now = Date.now();

    const instanceResults = yield* Effect.forEach(
      instances,
      (instance) =>
        Effect.gen(function* () {
          const providerOptions = providerStartOptionsFromInstance(instance);
          const optionsKey = JSON.stringify(providerOptions ?? null);
          const cached = cache.get(instance.instanceId);
          if (
            input.refresh !== true &&
            cached &&
            cached.expiresAt > now &&
            cached.optionsKey === optionsKey
          ) {
            return { instance, result: cached.result, warning: null } as const;
          }
          const adapter = options.providerAdapterRegistry.getByInstance
            ? yield* options.providerAdapterRegistry.getByInstance(instance.instanceId)
            : yield* options.providerAdapterRegistry.getByProvider("codex");
          if (!adapter.listExternalThreads) {
            return {
              instance,
              result: null,
              warning: "The installed Codex provider does not support thread discovery.",
            } as const;
          }
          const result = yield* adapter
            .listExternalThreads({
              providerInstanceId: instance.instanceId,
              ...(providerOptions ? { providerOptions } : {}),
              useStateDbOnly: input.refresh !== true,
              maxThreads: DISCOVERY_MAX_THREADS_PER_INSTANCE,
            })
            .pipe(
              Effect.map((result) => ({ result, warning: null as string | null })),
              Effect.catch((cause) =>
                Effect.succeed({ result: null, warning: errorMessage(cause) }),
              ),
            );
          if (result.result) {
            cache.set(instance.instanceId, {
              expiresAt: now + DISCOVERY_CACHE_TTL_MS,
              optionsKey,
              result: result.result,
            });
          }
          return { instance, ...result } as const;
        }),
      { concurrency: 2 },
    );

    const candidates: ExternalThreadCandidate[] = [];
    const seen = new Set<string>();
    let duplicateCount = 0;
    let adoptedCount = 0;
    for (const { instance, result } of instanceResults) {
      if (!result) continue;
      for (const thread of result.threads) {
        const externalKey = `${instance.instanceId}:${thread.externalThreadId}`;
        if (seen.has(externalKey)) {
          duplicateCount += 1;
          continue;
        }
        if (boundExternalKeys.has(externalKey)) {
          adoptedCount += 1;
          continue;
        }
        seen.add(externalKey);
        const match = yield* resolveExternalThreadWorkspaceMatch({
          cwd: thread.cwd,
          fileSystem: options.fileSystem,
          path: options.path,
          platform: options.platform,
          projects: shell.projects,
        });
        const title = thread.name?.trim() || thread.preview.trim() || "Untitled Codex task";
        candidates.push({
          provider: "codex",
          providerInstanceId: instance.instanceId,
          externalThreadId: thread.externalThreadId,
          title,
          preview: thread.preview,
          cwd: thread.cwd,
          sourceKind: thread.sourceKind,
          status: thread.status,
          modelProvider: thread.modelProvider,
          createdAt: toIsoDateTime(thread.createdAtSeconds),
          updatedAt: toIsoDateTime(thread.recencyAtSeconds ?? thread.updatedAtSeconds),
          matchedProjectId: match.projectId,
          matchKind: match.matchKind,
        });
      }
    }

    candidates.sort((left, right) => right.updatedAt.localeCompare(left.updatedAt));
    const warnings = instanceResults.flatMap(({ instance, warning }) =>
      warning ? [{ providerInstanceId: instance.instanceId, message: warning }] : [],
    );
    yield* Effect.logInfo("external Codex thread discovery completed", {
      durationMs: Date.now() - startedAt,
      instanceCount: instances.length,
      candidateCount: candidates.length,
      duplicateCount,
      adoptedCount,
      warningCount: warnings.length,
      refresh: input.refresh === true,
    });
    for (const warning of warnings) {
      yield* Effect.logWarning("external Codex thread discovery instance failed", warning);
    }
    return {
      candidates,
      refreshedAt: new Date(now).toISOString(),
      warnings,
      truncated: instanceResults.some(({ result }) => result?.truncated === true),
    } satisfies OrchestrationListExternalThreadsResult;
  });
}
