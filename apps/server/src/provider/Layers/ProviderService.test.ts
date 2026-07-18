// FILE: ProviderService.test.ts
// Purpose: Verifies cross-provider routing, persistence, recovery, and runtime lifecycle behavior.
// Layer: Provider service integration tests
// Depends on: ProviderServiceLive with in-memory adapter and SQLite fakes.

import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash } from "node:crypto";

import type {
  ProviderApprovalDecision,
  ProviderRuntimeEvent,
  ProviderSendTurnInput,
  ProviderSession,
  ProviderStartReviewInput,
  ProviderSteerTurnInput,
  ProviderTurnStartResult,
} from "@synara/contracts";
import {
  ApprovalRequestId,
  EventId,
  type ServerSettings,
  type ProviderKind,
  type ProviderInstanceId,
  ProviderSessionStartInput,
  ThreadId,
  TurnId,
} from "@synara/contracts";
import { it, assert, vi } from "@effect/vitest";
import { assertFailure } from "@effect/vitest/utils";

import { Deferred, Effect, Exit, Fiber, Layer, Option, PubSub, Ref, Scope, Stream } from "effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import {
  ProviderAdapterSessionNotFoundError,
  ProviderSessionDirectoryPersistenceError,
  ProviderUnsupportedError,
  ProviderValidationError,
  type ProviderAdapterError,
} from "../Errors.ts";
import type {
  ProviderAdapterForkThreadInput,
  ProviderAdapterSessionStartInput,
  ProviderAdapterShape,
} from "../Services/ProviderAdapter.ts";
import { ProviderAdapterRegistry } from "../Services/ProviderAdapterRegistry.ts";
import { ProviderService } from "../Services/ProviderService.ts";
import { ProviderSessionDirectory } from "../Services/ProviderSessionDirectory.ts";
import { makeProviderServiceLive } from "./ProviderService.ts";
import { ProviderSessionDirectoryLive } from "./ProviderSessionDirectory.ts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { ServerSecretStore } from "../../auth/Services/ServerSecretStore.ts";
import { ProviderSessionRuntimeRepositoryLive } from "../../persistence/Layers/ProviderSessionRuntime.ts";
import { ProviderSessionRuntimeRepository } from "../../persistence/Services/ProviderSessionRuntime.ts";
import {
  makeSqlitePersistenceLive,
  SqlitePersistenceMemory,
} from "../../persistence/Layers/Sqlite.ts";
import { AnalyticsService } from "../../telemetry/Services/AnalyticsService.ts";
import { ServerSettingsService } from "../../serverSettings.ts";
import {
  buildCodexProcessEnv,
  buildCodexProcessLaunchContext,
  isCodexSharedContinuationStatePrepared,
  readCodexSharedContinuationGeneration,
} from "../../codexProcessEnv.ts";
import { resolveActiveCodexHomeWritePath } from "../../codexHomePaths.ts";

const asRequestId = (value: string): ApprovalRequestId => ApprovalRequestId.makeUnsafe(value);
const asEventId = (value: string): EventId => EventId.makeUnsafe(value);
const asProviderInstanceId = (value: string): ProviderInstanceId => value as ProviderInstanceId;
const asThreadId = (value: string): ThreadId => ThreadId.makeUnsafe(value);
const asTurnId = (value: string): TurnId => TurnId.makeUnsafe(value);

function makeSharedCodexContinuationFixture(
  accountIds: readonly string[],
  preparedAccountIds: readonly string[] = accountIds,
) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "synara-provider-continuation-"));
  const homePath = path.join(root, "codex-home");
  const runtimeHomePath = path.join(root, "synara-runtime");
  const environment = { SYNARA_HOME: runtimeHomePath };
  fs.mkdirSync(homePath, { recursive: true });
  fs.writeFileSync(path.join(homePath, "config.toml"), "", "utf8");
  const shadowHomePaths = new Map<string, string>();
  const preparedAccounts = new Set(preparedAccountIds);
  for (const accountId of accountIds) {
    const shadowHomePath = path.join(root, `codex-shadow-${accountId}`);
    fs.mkdirSync(shadowHomePath, { recursive: true });
    fs.writeFileSync(path.join(shadowHomePath, "auth.json"), JSON.stringify({ accountId }), "utf8");
    shadowHomePaths.set(accountId, shadowHomePath);
    if (preparedAccounts.has(accountId)) {
      buildCodexProcessEnv({
        env: { ...process.env, ...environment },
        homePath,
        shadowHomePath,
        accountId,
      });
    }
  }
  return {
    root,
    homePath,
    environment,
    instanceEnvironment: Object.entries(environment).map(([name, value]) => ({
      name,
      value,
      sensitive: false,
    })),
    shadowHomePath: (accountId: string) => shadowHomePaths.get(accountId)!,
  };
}

function providerInstanceForSharedCodexFixture(
  fixture: ReturnType<typeof makeSharedCodexContinuationFixture>,
  accountId: string,
) {
  return {
    driver: "codex" as const,
    environment: fixture.instanceEnvironment,
    config: {
      homePath: fixture.homePath,
      shadowHomePath: fixture.shadowHomePath(accountId),
      accountId,
    },
  };
}

function requireSharedCodexFixtureGeneration(
  fixture: ReturnType<typeof makeSharedCodexContinuationFixture>,
  accountId: string,
): string {
  const generation = readCodexSharedContinuationGeneration({
    env: { ...process.env, ...fixture.environment },
    homePath: fixture.homePath,
    shadowHomePath: fixture.shadowHomePath(accountId),
    accountId,
  });
  if (!generation) {
    assert.fail(`Expected prepared Codex continuation generation for '${accountId}'`);
  }
  return generation;
}

const providerServiceSecretBytes = new Map<string, Uint8Array>();
const ProviderServiceTestSecretStoreLayer = Layer.succeed(ServerSecretStore, {
  get: (name) => Effect.succeed(providerServiceSecretBytes.get(name) ?? null),
  set: (name, value) =>
    Effect.sync(() => {
      providerServiceSecretBytes.set(name, Uint8Array.from(value));
    }),
  getOrCreateRandom: (name, bytes) =>
    Effect.sync(() => {
      const existing = providerServiceSecretBytes.get(name);
      if (existing) return existing;
      const generated = Uint8Array.from({ length: bytes }, (_, index) => (index * 17 + 23) % 256);
      providerServiceSecretBytes.set(name, generated);
      return generated;
    }),
  remove: (name) =>
    Effect.sync(() => {
      providerServiceSecretBytes.delete(name);
    }),
});

type LegacyProviderRuntimeEvent = {
  readonly type: string;
  readonly eventId: EventId;
  readonly provider: ProviderKind;
  readonly createdAt: string;
  readonly threadId: ThreadId;
  readonly turnId?: string | undefined;
  readonly itemId?: string | undefined;
  readonly requestId?: string | undefined;
  readonly payload?: unknown | undefined;
  readonly [key: string]: unknown;
};

type ReleaseListSessions = (sessions: ReadonlyArray<ProviderSession>) => void;

// Converts deferred listSessions callbacks into typed release handles for race tests.
function requireReleaseListSessions(release: ReleaseListSessions | undefined): ReleaseListSessions {
  if (typeof release !== "function") {
    assert.fail("Expected listSessions release callback");
  }
  return release;
}

function withoutResumeCursor(session: ProviderSession): ProviderSession {
  const { resumeCursor: _omittedResumeCursor, ...rest } = session;
  return rest;
}

function asRuntimePayloadRecord(value: unknown): Record<string, unknown> {
  return value !== null && typeof value === "object" ? (value as Record<string, unknown>) : {};
}

function makeFakeCodexAdapter(
  provider: ProviderKind = "codex",
  options?: { readonly conversationRollback?: "native" | "restart-session" },
) {
  const sessions = new Map<ThreadId, ProviderSession>();
  const runtimeEventPubSub = Effect.runSync(PubSub.unbounded<ProviderRuntimeEvent>());
  let beforeStartSession: ((input: ProviderAdapterSessionStartInput) => void) | undefined;

  const startSession = vi.fn((input: ProviderAdapterSessionStartInput) =>
    Effect.sync(() => {
      beforeStartSession?.(input);
      const now = new Date().toISOString();
      const session: ProviderSession = {
        provider,
        status: "ready",
        runtimeMode: input.runtimeMode,
        threadId: input.threadId,
        ...(input.providerInstanceId ? { providerInstanceId: input.providerInstanceId } : {}),
        resumeCursor: input.resumeCursor ?? { opaque: `resume-${String(input.threadId)}` },
        cwd: input.cwd ?? process.cwd(),
        createdAt: now,
        updatedAt: now,
      };
      sessions.set(session.threadId, session);
      return session;
    }),
  );

  const sendTurn = vi.fn(
    (
      input: ProviderSendTurnInput,
    ): Effect.Effect<ProviderTurnStartResult, ProviderAdapterError> => {
      if (!sessions.has(input.threadId)) {
        return Effect.fail(
          new ProviderAdapterSessionNotFoundError({
            provider,
            threadId: input.threadId,
          }),
        );
      }

      return Effect.succeed({
        threadId: input.threadId,
        turnId: TurnId.makeUnsafe(`turn-${String(input.threadId)}`),
      });
    },
  );

  const steerTurn = vi.fn(
    (input: ProviderSteerTurnInput): Effect.Effect<ProviderTurnStartResult, ProviderAdapterError> =>
      Effect.succeed({
        threadId: input.threadId,
        turnId: TurnId.makeUnsafe(`steer-${String(input.threadId)}`),
      }),
  );

  const startReview = vi.fn(
    (
      input: ProviderStartReviewInput,
    ): Effect.Effect<ProviderTurnStartResult, ProviderAdapterError> =>
      Effect.succeed({
        threadId: input.threadId,
        turnId: TurnId.makeUnsafe(`review-${String(input.threadId)}`),
      }),
  );

  const interruptTurn = vi.fn(
    (
      _threadId: ThreadId,
      _turnId?: TurnId,
      _providerThreadId?: string,
    ): Effect.Effect<void, ProviderAdapterError> => Effect.void,
  );

  const respondToRequest = vi.fn(
    (
      _threadId: ThreadId,
      _requestId: string,
      _decision: ProviderApprovalDecision,
    ): Effect.Effect<void, ProviderAdapterError> => Effect.void,
  );

  const respondToUserInput = vi.fn(
    (
      _threadId: ThreadId,
      _requestId: string,
      _answers: Record<string, unknown>,
    ): Effect.Effect<void, ProviderAdapterError> => Effect.void,
  );

  const stopSession = vi.fn(
    (threadId: ThreadId): Effect.Effect<void, ProviderAdapterError> =>
      Effect.sync(() => {
        sessions.delete(threadId);
      }),
  );

  const listSessions = vi.fn(
    (): Effect.Effect<ReadonlyArray<ProviderSession>> =>
      Effect.sync(() => Array.from(sessions.values())),
  );

  const hasSession = vi.fn(
    (threadId: ThreadId): Effect.Effect<boolean> => Effect.succeed(sessions.has(threadId)),
  );

  const readThread = vi.fn(
    (
      threadId: ThreadId,
    ): Effect.Effect<
      {
        threadId: ThreadId;
        turns: ReadonlyArray<{ id: TurnId; items: readonly [] }>;
      },
      ProviderAdapterError
    > =>
      Effect.succeed({
        threadId,
        turns: [{ id: asTurnId("turn-1"), items: [] }],
      }),
  );

  const rollbackThread = vi.fn(
    (
      threadId: ThreadId,
      _numTurns: number,
    ): Effect.Effect<{ threadId: ThreadId; turns: readonly [] }, ProviderAdapterError> =>
      Effect.succeed({ threadId, turns: [] }),
  );

  const compactThread = vi.fn(
    (_threadId: ThreadId): Effect.Effect<void, ProviderAdapterError> => Effect.void,
  );

  const forkThread = vi.fn((input: ProviderAdapterForkThreadInput) =>
    Effect.succeed({
      threadId: input.threadId,
      resumeCursor: { opaque: `fork-${String(input.threadId)}` },
    }),
  );

  const stopAll = vi.fn(
    (): Effect.Effect<void, ProviderAdapterError> =>
      Effect.sync(() => {
        sessions.clear();
      }),
  );

  const adapter: ProviderAdapterShape<ProviderAdapterError> = {
    provider,
    capabilities: {
      sessionModelSwitch: "in-session",
      supportsTurnSteering: true,
      ...(options?.conversationRollback
        ? { conversationRollback: options.conversationRollback }
        : {}),
    },
    startSession,
    sendTurn,
    steerTurn,
    startReview,
    interruptTurn,
    respondToRequest,
    respondToUserInput,
    stopSession,
    listSessions,
    hasSession,
    readThread,
    rollbackThread,
    compactThread,
    forkThread,
    stopAll,
    streamEvents: Stream.fromPubSub(runtimeEventPubSub),
  };

  const emit = (event: LegacyProviderRuntimeEvent): void => {
    Effect.runSync(PubSub.publish(runtimeEventPubSub, event as unknown as ProviderRuntimeEvent));
  };

  const waitForRuntimeSubscribers = (count = 1): Effect.Effect<void> =>
    waitUntil(
      () => runtimeEventPubSub.subscribers.size >= count,
      500,
      20,
      `${provider} runtime event subscriber`,
    );

  const updateSession = (
    threadId: ThreadId,
    update: (session: ProviderSession) => ProviderSession,
  ): void => {
    const existing = sessions.get(threadId);
    if (!existing) {
      return;
    }
    sessions.set(threadId, update(existing));
  };

  return {
    adapter,
    emit,
    waitForRuntimeSubscribers,
    updateSession,
    setBeforeStartSession: (
      hook: ((input: ProviderAdapterSessionStartInput) => void) | undefined,
    ) => {
      beforeStartSession = hook;
    },
    startSession,
    sendTurn,
    steerTurn,
    startReview,
    interruptTurn,
    respondToRequest,
    respondToUserInput,
    stopSession,
    listSessions,
    hasSession,
    readThread,
    rollbackThread,
    compactThread,
    forkThread,
    stopAll,
  };
}

const sleep = (ms: number) =>
  Effect.promise(() => new Promise<void>((resolve) => setTimeout(resolve, ms)));

const waitUntil = (
  predicate: () => boolean,
  timeoutMs = 500,
  intervalMs = 20,
  description = "condition",
): Effect.Effect<void> =>
  Effect.gen(function* () {
    const deadline = Date.now() + timeoutMs;
    while (!predicate() && Date.now() < deadline) {
      yield* sleep(intervalMs);
    }
    if (!predicate()) {
      assert.fail(`Timed out waiting for ${description}`);
    }
  });

const waitUntilEffect = <E = never, R = never>(
  predicate: () => Effect.Effect<boolean, E, R>,
  timeoutMs = 500,
  intervalMs = 20,
  description = "condition",
): Effect.Effect<void, E, R> =>
  Effect.gen(function* () {
    const deadline = Date.now() + timeoutMs;
    let matched = yield* predicate();
    while (!matched && Date.now() < deadline) {
      yield* sleep(intervalMs);
      matched = yield* predicate();
    }
    if (!matched) {
      assert.fail(`Timed out waiting for ${description}`);
    }
  });

function makeProviderServiceLayer(
  options?: Parameters<typeof makeProviderServiceLive>[0],
  settings?: Partial<ServerSettings>,
  providers?: {
    readonly includeRestartRollbackDroid?: boolean;
    readonly includePi?: boolean;
  },
) {
  const codex = makeFakeCodexAdapter();
  const claude = makeFakeCodexAdapter("claudeAgent");
  const antigravity = makeFakeCodexAdapter("antigravity");
  const droid = makeFakeCodexAdapter("droid", { conversationRollback: "restart-session" });
  const pi = makeFakeCodexAdapter("pi");
  const registry: typeof ProviderAdapterRegistry.Service = {
    getByProvider: (provider) =>
      provider === "codex"
        ? Effect.succeed(codex.adapter)
        : provider === "claudeAgent"
          ? Effect.succeed(claude.adapter)
          : provider === "antigravity"
            ? Effect.succeed(antigravity.adapter)
            : provider === "droid" && providers?.includeRestartRollbackDroid === true
              ? Effect.succeed(droid.adapter)
              : provider === "pi" && providers?.includePi === true
                ? Effect.succeed(pi.adapter)
                : Effect.fail(new ProviderUnsupportedError({ provider })),
    listProviders: () =>
      Effect.succeed([
        "codex",
        "claudeAgent",
        "antigravity",
        ...(providers?.includeRestartRollbackDroid === true ? (["droid"] as const) : []),
        ...(providers?.includePi === true ? (["pi"] as const) : []),
      ] as const),
  };

  const providerAdapterLayer = Layer.succeed(ProviderAdapterRegistry, registry);
  const runtimeRepositoryLayer = ProviderSessionRuntimeRepositoryLive.pipe(
    Layer.provide(SqlitePersistenceMemory),
  );
  const directoryLayer = ProviderSessionDirectoryLive.pipe(Layer.provide(runtimeRepositoryLayer));
  const serverSettingsLayer = ServerSettingsService.layerTest(settings);

  const rawLayer = Layer.mergeAll(
      makeProviderServiceLive(options).pipe(
        Layer.provide(providerAdapterLayer),
        Layer.provide(directoryLayer),
        Layer.provideMerge(AnalyticsService.layerTest),
        Layer.provide(serverSettingsLayer),
        Layer.provide(ProviderServiceTestSecretStoreLayer),
      ),
      serverSettingsLayer,
      directoryLayer,

      runtimeRepositoryLayer,
      NodeServices.layer,
  );
  const layer = it.layer(rawLayer);

  return {
    codex,
    claude,
    antigravity,
    droid,
    pi,
    layer,
    rawLayer,
  };
}

const routing = makeProviderServiceLayer();
const disabledRouting = makeProviderServiceLayer(undefined, {
  providerInstances: {
    claude_disabled: {
      driver: "claudeAgent",
      enabled: false,
      config: {
        homePath: "/tmp/claude-disabled",
      },
    },
  },
});
const restartRollbackRouting = makeProviderServiceLayer(undefined, undefined, {
  includeRestartRollbackDroid: true,
});
const piInteractionRouting = makeProviderServiceLayer(undefined, undefined, { includePi: true });

routing.layer("ProviderServiceLive native forks", (it) => {
  it.effect("forks across Codex account instances sharing continuation storage", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService;
      const serverSettings = yield* ServerSettingsService;
      const directory = yield* ProviderSessionDirectory;
      const sourceThreadId = asThreadId("thread-fork-source-personal");
      const targetThreadId = asThreadId("thread-fork-target-work");
      const fixture = makeSharedCodexContinuationFixture(["personal", "work"], ["personal"]);
      const sharedHomePath = fixture.homePath;

      yield* serverSettings.updateSettings({
        providerInstances: {
          codex_personal: {
            driver: "codex",
            environment: fixture.instanceEnvironment,
            config: {
              homePath: sharedHomePath,
              shadowHomePath: fixture.shadowHomePath("personal"),
              accountId: "personal",
            },
          },
          codex_work: {
            driver: "codex",
            environment: fixture.instanceEnvironment,
            config: {
              homePath: sharedHomePath,
              shadowHomePath: fixture.shadowHomePath("work"),
              accountId: "work",
            },
          },
        },
      });

      const source = yield* provider.startSession(sourceThreadId, {
        provider: "codex",
        providerInstanceId: "codex_personal",
        threadId: sourceThreadId,
        runtimeMode: "full-access",
      });
      assert.equal(
        isCodexSharedContinuationStatePrepared({
          env: { ...process.env, ...fixture.environment },
          homePath: sharedHomePath,
          shadowHomePath: fixture.shadowHomePath("work"),
          accountId: "work",
        }),
        false,
      );
      routing.codex.forkThread.mockClear();

      assert.equal(typeof provider.forkThread, "function");
      if (!provider.forkThread) {
        return;
      }

      const result = yield* provider.forkThread({
        sourceThreadId,
        threadId: targetThreadId,
        modelSelection: {
          instanceId: "codex_work",
          model: "gpt-5.4",
        },
        runtimeMode: "full-access",
      });

      assert.notEqual(result, null);
      assert.equal(
        isCodexSharedContinuationStatePrepared({
          env: { ...process.env, ...fixture.environment },
          homePath: sharedHomePath,
          shadowHomePath: fixture.shadowHomePath("work"),
          accountId: "work",
        }),
        true,
      );
      assert.equal(routing.codex.forkThread.mock.calls.length, 1);
      const forkInput = routing.codex.forkThread.mock.calls[0]?.[0];
      assert.deepEqual(forkInput?.sourceResumeCursor, source.resumeCursor);
      assert.equal(
        forkInput?.expectedCodexContinuationGeneration,
        readCodexSharedContinuationGeneration({
          env: { ...process.env, ...fixture.environment },
          homePath: sharedHomePath,
          shadowHomePath: fixture.shadowHomePath("work"),
          accountId: "work",
        }),
      );
      assert.equal(forkInput?.modelSelection?.instanceId, "codex_work");
      assert.deepEqual(forkInput?.providerOptions, {
        codex: {
          homePath: sharedHomePath,
          shadowHomePath: fixture.shadowHomePath("work"),
          accountId: "work",
          environment: fixture.environment,
        },
      });
      const targetBinding = Option.getOrUndefined(yield* directory.getBinding(targetThreadId));
      assert.ok(targetBinding);
      assert.match(
        String(
          targetBinding.runtimePayload && typeof targetBinding.runtimePayload === "object"
            ? (targetBinding.runtimePayload as Record<string, unknown>).continuationIdentity
            : "",
        ),
        /^codex:shared-v2:[0-9a-f-]{36}:/,
      );
      assert.equal(
        targetBinding.runtimePayload && typeof targetBinding.runtimePayload === "object"
          ? (targetBinding.runtimePayload as Record<string, unknown>).continuationResetRequested
          : undefined,
        undefined,
      );
      fs.rmSync(fixture.root, { recursive: true, force: true });
    }),
  );

  it.effect("does not recreate damaged shared source state before a native fork", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService;
      const serverSettings = yield* ServerSettingsService;
      const sourceThreadId = asThreadId("thread-fork-damaged-source");
      const targetThreadId = asThreadId("thread-fork-damaged-source-target");
      const fixture = makeSharedCodexContinuationFixture(["personal", "work"], ["personal"]);
      yield* serverSettings.updateSettings({
        providerInstances: {
          codex_personal: {
            driver: "codex",
            environment: fixture.instanceEnvironment,
            config: {
              homePath: fixture.homePath,
              shadowHomePath: fixture.shadowHomePath("personal"),
              accountId: "personal",
            },
          },
          codex_work: {
            driver: "codex",
            environment: fixture.instanceEnvironment,
            config: {
              homePath: fixture.homePath,
              shadowHomePath: fixture.shadowHomePath("work"),
              accountId: "work",
            },
          },
        },
      });
      yield* provider.startSession(sourceThreadId, {
        provider: "codex",
        providerInstanceId: "codex_personal",
        threadId: sourceThreadId,
        runtimeMode: "full-access",
      });
      const workOverlayHomePath = resolveActiveCodexHomeWritePath({
        env: { ...process.env, ...fixture.environment },
        homePath: fixture.homePath,
        shadowHomePath: fixture.shadowHomePath("work"),
        accountId: "work",
      });
      assert.equal(fs.existsSync(workOverlayHomePath), false);
      const sourceSessionsPath = path.join(fixture.homePath, "sessions");
      fs.rmSync(sourceSessionsPath, { recursive: true, force: true });
      routing.codex.forkThread.mockClear();

      assert.equal(typeof provider.forkThread, "function");
      if (!provider.forkThread) return;
      const result = yield* Effect.result(
        provider.forkThread({
          sourceThreadId,
          threadId: targetThreadId,
          modelSelection: { instanceId: "codex_work", model: "gpt-5.4" },
          runtimeMode: "full-access",
        }),
      );

      assert.equal(result._tag, "Failure");
      if (result._tag === "Failure") {
        assert.match(String(result.failure), /missing (?:or damaged|'sessions')/);
      }
      assert.equal(routing.codex.forkThread.mock.calls.length, 0);
      assert.equal(fs.existsSync(sourceSessionsPath), false);
      assert.equal(fs.existsSync(workOverlayHomePath), false);
      fs.rmSync(fixture.root, { recursive: true, force: true });
    }),
  );

  it.effect("falls back without invoking native fork across incompatible Codex homes", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService;
      const serverSettings = yield* ServerSettingsService;
      const sourceThreadId = asThreadId("thread-fork-source-home-a");
      const targetThreadId = asThreadId("thread-fork-target-home-b");

      yield* serverSettings.updateSettings({
        providerInstances: {
          codex_personal: {
            driver: "codex",
            config: {
              homePath: "/tmp/codex-fork-home-a",
              shadowHomePath: "/tmp/codex-fork-home-a-auth",
              accountId: "personal",
            },
          },
          codex_work: {
            driver: "codex",
            config: {
              homePath: "/tmp/codex-fork-home-b",
              shadowHomePath: "/tmp/codex-fork-home-b-auth",
              accountId: "work",
            },
          },
        },
      });

      yield* provider.startSession(sourceThreadId, {
        provider: "codex",
        providerInstanceId: "codex_personal",
        threadId: sourceThreadId,
        runtimeMode: "full-access",
      });
      routing.codex.forkThread.mockClear();

      assert.equal(typeof provider.forkThread, "function");
      if (!provider.forkThread) {
        return;
      }

      const result = yield* provider.forkThread({
        sourceThreadId,
        threadId: targetThreadId,
        modelSelection: {
          instanceId: "codex_work",
          model: "gpt-5.4",
        },
        runtimeMode: "full-access",
      });

      assert.equal(result, null);
      assert.equal(routing.codex.forkThread.mock.calls.length, 0);
    }),
  );
});

it.effect("ProviderServiceLive keeps persisted resumable sessions on startup", () =>
  Effect.gen(function* () {
    const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "synara-provider-service-"));
    const dbPath = path.join(tempDir, "orchestration.sqlite");

    const codex = makeFakeCodexAdapter();
    const registry: typeof ProviderAdapterRegistry.Service = {
      getByProvider: (provider) =>
        provider === "codex"
          ? Effect.succeed(codex.adapter)
          : Effect.fail(new ProviderUnsupportedError({ provider })),
      listProviders: () => Effect.succeed(["codex"]),
    };

    const persistenceLayer = makeSqlitePersistenceLive(dbPath);
    const runtimeRepositoryLayer = ProviderSessionRuntimeRepositoryLive.pipe(
      Layer.provide(persistenceLayer),
    );
    const directoryLayer = ProviderSessionDirectoryLive.pipe(Layer.provide(runtimeRepositoryLayer));

    yield* Effect.gen(function* () {
      const directory = yield* ProviderSessionDirectory;
      yield* directory.upsert({
        provider: "codex",
        providerInstanceId: "codex",
        threadId: ThreadId.makeUnsafe("thread-stale"),
      });
    }).pipe(Effect.provide(directoryLayer));

    const providerLayer = makeProviderServiceLive().pipe(
      Layer.provide(Layer.succeed(ProviderAdapterRegistry, registry)),
      Layer.provide(directoryLayer),
      Layer.provide(AnalyticsService.layerTest),
      Layer.provide(ServerSettingsService.layerTest()),
      Layer.provide(ProviderServiceTestSecretStoreLayer),
    );

    yield* Effect.gen(function* () {
      yield* ProviderService;
    }).pipe(Effect.provide(providerLayer));

    const persistedProvider = yield* Effect.gen(function* () {
      const directory = yield* ProviderSessionDirectory;
      return yield* directory.getProvider(asThreadId("thread-stale"));
    }).pipe(Effect.provide(directoryLayer));
    assert.equal(persistedProvider, "codex");

    const runtime = yield* Effect.gen(function* () {
      const repository = yield* ProviderSessionRuntimeRepository;
      return yield* repository.getByThreadId({ threadId: asThreadId("thread-stale") });
    }).pipe(Effect.provide(runtimeRepositoryLayer));
    assert.equal(Option.isSome(runtime), true);

    const legacyTableRows = yield* Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      return yield* sql<{ readonly name: string }>`
        SELECT name
        FROM sqlite_master
        WHERE type = 'table' AND name = 'provider_sessions'
      `;
    }).pipe(Effect.provide(persistenceLayer));
    assert.equal(legacyTableRows.length, 0);

    fs.rmSync(tempDir, { recursive: true, force: true });
  }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect(
  "ProviderServiceLive persists active sessions as stopped before adapter cleanup runs",
  () =>
    Effect.gen(function* () {
      const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "synara-provider-service-stopall-"));
      const dbPath = path.join(tempDir, "orchestration.sqlite");
      const persistenceLayer = makeSqlitePersistenceLive(dbPath);
      const runtimeRepositoryLayer = ProviderSessionRuntimeRepositoryLive.pipe(
        Layer.provide(persistenceLayer),
      );

      const codex = makeFakeCodexAdapter();
      const threadId = asThreadId("thread-stopall");
      const providerInstanceId = asProviderInstanceId("codex_work");
      const resumeCursor = {
        threadId,
        resume: "resume-session-stopall",
        resumeSessionAt: "assistant-message-stopall",
        turnCount: 1,
      };
      codex.stopAll.mockImplementation(() =>
        Effect.fail(
          new ProviderAdapterSessionNotFoundError({
            provider: "codex",
            threadId,
          }),
        ),
      );

      const registry: typeof ProviderAdapterRegistry.Service = {
        getByProvider: (provider) =>
          provider === "codex"
            ? Effect.succeed(codex.adapter)
            : Effect.fail(new ProviderUnsupportedError({ provider })),
        listProviders: () => Effect.succeed(["codex"]),
      };

      const providerLayer = makeProviderServiceLive().pipe(
        Layer.provide(Layer.succeed(ProviderAdapterRegistry, registry)),
        Layer.provide(ProviderSessionDirectoryLive.pipe(Layer.provide(runtimeRepositoryLayer))),
        Layer.provide(AnalyticsService.layerTest),
        Layer.provide(
          ServerSettingsService.layerTest({
            providerInstances: {
              codex_work: {
                driver: "codex",
                config: {
                  homePath: "/tmp/codex-work",
                },
              },
            },
          }),
        ),
        Layer.provide(ProviderServiceTestSecretStoreLayer),
      );

      yield* Effect.gen(function* () {
        const provider = yield* ProviderService;
        yield* provider.startSession(threadId, {
          provider: "codex",
          providerInstanceId,
          cwd: "/tmp/project",
          runtimeMode: "full-access",
          threadId,
        });
        codex.updateSession(threadId, (existing) => {
          const { providerInstanceId: _providerInstanceId, ...sessionWithoutInstance } = existing;
          return {
            ...sessionWithoutInstance,
            status: "running",
            activeTurnId: asTurnId("turn-stopall"),
            resumeCursor,
          };
        });
      }).pipe(Effect.provide(providerLayer));

      const persisted = yield* Effect.gen(function* () {
        const repository = yield* ProviderSessionRuntimeRepository;
        return yield* repository.getByThreadId({ threadId });
      }).pipe(Effect.provide(runtimeRepositoryLayer));

      assert.equal(Option.isSome(persisted), true);
      if (Option.isSome(persisted)) {
        const runtimePayload = persisted.value.runtimePayload as Record<string, unknown>;
        assert.equal(persisted.value.status, "stopped");
        assert.equal(persisted.value.providerInstanceId, providerInstanceId);
        assert.deepEqual(persisted.value.resumeCursor, resumeCursor);
        assert.equal(runtimePayload.activeTurnId, null);
        assert.equal(runtimePayload.lastRuntimeEvent, "provider.stopAll");
      }

      fs.rmSync(tempDir, { recursive: true, force: true });
    }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect(
  "ProviderServiceLive restores rollback routing after restart using persisted thread mapping",
  () =>
    Effect.gen(function* () {
      const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "synara-provider-service-restart-"));
      const dbPath = path.join(tempDir, "orchestration.sqlite");
      const fixture = makeSharedCodexContinuationFixture(["default"]);
      const providerInstanceId = asProviderInstanceId("codex_restart");
      const serverSettingsLayer = ServerSettingsService.layerTest({
        providerInstances: {
          codex_restart: providerInstanceForSharedCodexFixture(fixture, "default"),
        },
      });
      const persistenceLayer = makeSqlitePersistenceLive(dbPath);
      const runtimeRepositoryLayer = ProviderSessionRuntimeRepositoryLive.pipe(
        Layer.provide(persistenceLayer),
      );

      const firstCodex = makeFakeCodexAdapter();
      const firstRegistry: typeof ProviderAdapterRegistry.Service = {
        getByProvider: (provider) =>
          provider === "codex"
            ? Effect.succeed(firstCodex.adapter)
            : Effect.fail(new ProviderUnsupportedError({ provider })),
        listProviders: () => Effect.succeed(["codex"]),
      };

      const firstDirectoryLayer = ProviderSessionDirectoryLive.pipe(
        Layer.provide(runtimeRepositoryLayer),
      );
      const firstProviderLayer = makeProviderServiceLive().pipe(
        Layer.provide(Layer.succeed(ProviderAdapterRegistry, firstRegistry)),
        Layer.provide(firstDirectoryLayer),
        Layer.provide(AnalyticsService.layerTest),
        Layer.provide(serverSettingsLayer),
        Layer.provide(ProviderServiceTestSecretStoreLayer),
      );
      const updatedResumeCursor = {
        threadId: asThreadId("thread-1"),
        resume: "resume-session-1",
        resumeSessionAt: "assistant-message-1",
        turnCount: 1,
      };

      const startedSession = yield* Effect.gen(function* () {
        const provider = yield* ProviderService;
        const threadId = asThreadId("thread-1");
        const session = yield* provider.startSession(threadId, {
          provider: "codex",
          cwd: "/tmp/project",
          providerInstanceId,
          runtimeMode: "full-access",
          threadId,
        });
        firstCodex.updateSession(threadId, (existing) => ({
          ...existing,
          status: "ready",
          resumeCursor: updatedResumeCursor,
          updatedAt: new Date(Date.now() + 1_000).toISOString(),
        }));
        return session;
      }).pipe(Effect.provide(firstProviderLayer));

      const persistedAfterStopAll = yield* Effect.gen(function* () {
        const repository = yield* ProviderSessionRuntimeRepository;
        return yield* repository.getByThreadId({ threadId: startedSession.threadId });
      }).pipe(Effect.provide(runtimeRepositoryLayer));
      assert.equal(Option.isSome(persistedAfterStopAll), true);
      if (Option.isSome(persistedAfterStopAll)) {
        assert.equal(persistedAfterStopAll.value.status, "stopped");
        assert.deepEqual(persistedAfterStopAll.value.resumeCursor, updatedResumeCursor);
      }

      const secondCodex = makeFakeCodexAdapter();
      const secondRegistry: typeof ProviderAdapterRegistry.Service = {
        getByProvider: (provider) =>
          provider === "codex"
            ? Effect.succeed(secondCodex.adapter)
            : Effect.fail(new ProviderUnsupportedError({ provider })),
        listProviders: () => Effect.succeed(["codex"]),
      };
      const secondDirectoryLayer = ProviderSessionDirectoryLive.pipe(
        Layer.provide(runtimeRepositoryLayer),
      );
      const secondProviderLayer = makeProviderServiceLive().pipe(
        Layer.provide(Layer.succeed(ProviderAdapterRegistry, secondRegistry)),
        Layer.provide(secondDirectoryLayer),
        Layer.provide(AnalyticsService.layerTest),
        Layer.provide(serverSettingsLayer),
        Layer.provide(ProviderServiceTestSecretStoreLayer),
      );

      secondCodex.startSession.mockClear();
      secondCodex.rollbackThread.mockClear();

      yield* Effect.gen(function* () {
        const provider = yield* ProviderService;
        yield* provider.rollbackConversation({
          threadId: startedSession.threadId,
          numTurns: 1,
        });
      }).pipe(Effect.provide(secondProviderLayer));

      assert.equal(secondCodex.startSession.mock.calls.length, 1);
      const resumedStartInput = secondCodex.startSession.mock.calls[0]?.[0];
      assert.equal(typeof resumedStartInput === "object" && resumedStartInput !== null, true);
      if (resumedStartInput && typeof resumedStartInput === "object") {
        const startPayload = resumedStartInput as {
          provider?: string;
          cwd?: string;
          expectedCodexContinuationGeneration?: string;
          resumeCursor?: unknown;
          threadId?: string;
        };
        assert.equal(startPayload.provider, "codex");
        assert.equal(startPayload.cwd, "/tmp/project");
        assert.deepEqual(startPayload.resumeCursor, updatedResumeCursor);
        assert.equal(startPayload.threadId, startedSession.threadId);
        assert.equal(
          startPayload.expectedCodexContinuationGeneration,
          requireSharedCodexFixtureGeneration(fixture, "default"),
        );
      }
      assert.equal(secondCodex.rollbackThread.mock.calls.length, 1);
      const rollbackCall = secondCodex.rollbackThread.mock.calls[0];
      assert.equal(typeof rollbackCall?.[0], "string");
      assert.equal(rollbackCall?.[1], 1);

      fs.rmSync(tempDir, { recursive: true, force: true });
      fs.rmSync(fixture.root, { recursive: true, force: true });
    }).pipe(Effect.provide(NodeServices.layer)),
);

disabledRouting.layer("ProviderServiceLive disabled provider instances", (it) => {
  it.effect("rejects disabled provider instances before adapter launch", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService;
      disabledRouting.claude.startSession.mockClear();

      const result = yield* Effect.result(
        provider.startSession(asThreadId("thread-disabled"), {
          provider: "claudeAgent",
          providerInstanceId: "claude_disabled",
          threadId: asThreadId("thread-disabled"),
          runtimeMode: "full-access",
        }),
      );

      assertFailure(
        result,
        new ProviderValidationError({
          operation: "ProviderService.startSession",
          issue: "Provider instance 'claude_disabled' is disabled.",
        }),
      );
      assert.equal(disabledRouting.claude.startSession.mock.calls.length, 0);
    }),
  );
});

const deletedRouting = makeProviderServiceLayer();

deletedRouting.layer("ProviderServiceLive deleted provider instances", (it) => {
  it.effect("stops sessions whose provider instance was deleted from settings", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService;
      const serverSettings = yield* ServerSettingsService;
      const directory = yield* ProviderSessionDirectory;
      const threadId = asThreadId("thread-deleted-instance");

      yield* serverSettings.updateSettings({
        providerInstances: {
          codex_work: {
            driver: "codex",
            displayName: "Codex Work",
          },
        },
      });

      yield* provider.startSession(threadId, {
        provider: "codex",
        providerInstanceId: "codex_work",
        threadId,
        runtimeMode: "full-access",
      });

      yield* serverSettings.updateSettings({ providerInstances: {} });
      deletedRouting.codex.stopSession.mockClear();

      yield* provider.stopSession({ threadId });

      assert.equal(deletedRouting.codex.stopSession.mock.calls.length, 1);
      assert.deepEqual(deletedRouting.codex.stopSession.mock.calls[0]?.[0], threadId);
      const binding = yield* directory.getBinding(threadId);
      assert.equal(Option.isNone(binding), true);
    }),
  );

  it.effect("stops sessions after their provider instance is disabled", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService;
      const serverSettings = yield* ServerSettingsService;
      const directory = yield* ProviderSessionDirectory;
      const threadId = asThreadId("thread-disabled-instance-cleanup");

      yield* serverSettings.updateSettings({
        providerInstances: {
          codex_cleanup: {
            driver: "codex",
            enabled: true,
          },
        },
      });
      yield* provider.startSession(threadId, {
        provider: "codex",
        providerInstanceId: "codex_cleanup",
        threadId,
        runtimeMode: "full-access",
      });
      yield* serverSettings.updateSettings({
        providerInstances: {
          codex_cleanup: {
            driver: "codex",
            enabled: false,
          },
        },
      });
      deletedRouting.codex.stopSession.mockClear();

      yield* provider.stopSession({ threadId });

      assert.equal(deletedRouting.codex.stopSession.mock.calls.length, 1);
      assert.equal(Option.isNone(yield* directory.getBinding(threadId)), true);
    }),
  );

  it.effect("removes stopped bindings even when no live adapter session remains", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService;
      const directory = yield* ProviderSessionDirectory;
      const threadId = asThreadId("thread-stopped-binding-cleanup");

      yield* directory.upsert({
        threadId,
        provider: "codex",
        providerInstanceId: "codex",
        runtimeMode: "full-access",
        status: "stopped",
      });
      deletedRouting.codex.stopSession.mockClear();

      yield* provider.stopSession({ threadId });

      assert.equal(deletedRouting.codex.stopSession.mock.calls.length, 0);
      assert.equal(Option.isNone(yield* directory.getBinding(threadId)), true);
    }),
  );
});

it.effect(
  "ProviderServiceLive stops runtime sessions through the bound provider when instance ids are reused",
  () =>
    Effect.gen(function* () {
      const codex = makeFakeCodexAdapter("codex");
      const claude = makeFakeCodexAdapter("claudeAgent");
      const threadId = asThreadId("thread-bound-provider-cleanup");
      const providerInstanceId = asProviderInstanceId("work");
      const runtimeRepositoryLayer = ProviderSessionRuntimeRepositoryLive.pipe(
        Layer.provide(SqlitePersistenceMemory),
      );
      const directoryLayer = ProviderSessionDirectoryLive.pipe(
        Layer.provide(runtimeRepositoryLayer),
      );
      const registry: typeof ProviderAdapterRegistry.Service = {
        getByInstance: (instanceId) =>
          instanceId === providerInstanceId
            ? Effect.succeed(claude.adapter)
            : Effect.fail(new ProviderUnsupportedError({ provider: String(instanceId) })),
        getByProvider: (provider) =>
          provider === "codex"
            ? Effect.succeed(codex.adapter)
            : provider === "claudeAgent"
              ? Effect.succeed(claude.adapter)
              : Effect.fail(new ProviderUnsupportedError({ provider })),
        listProviders: () => Effect.succeed(["codex", "claudeAgent"]),
      };

      const providerLayer = makeProviderServiceLive().pipe(
        Layer.provide(Layer.succeed(ProviderAdapterRegistry, registry)),
        Layer.provide(directoryLayer),
        Layer.provide(AnalyticsService.layerTest),
        Layer.provide(
          ServerSettingsService.layerTest({
            providerInstances: {
              work: {
                driver: "claudeAgent",
                enabled: true,
              },
            },
          }),
        ),
        Layer.provide(ProviderServiceTestSecretStoreLayer),
      );

      yield* Effect.gen(function* () {
        const provider = yield* ProviderService;
        const directory = yield* ProviderSessionDirectory;
        yield* codex.adapter.startSession({
          provider: "codex",
          providerInstanceId,
          threadId,
          cwd: "/tmp/project-bound-provider-cleanup",
          runtimeMode: "full-access",
        });
        yield* directory.upsert({
          threadId,
          provider: "codex",
          providerInstanceId,
          runtimeMode: "full-access",
          status: "running",
        });
        assert.equal(typeof provider.stopRuntimeSession, "function");
        if (provider.stopRuntimeSession) {
          yield* provider.stopRuntimeSession({ threadId });
        }
      }).pipe(Effect.provide(Layer.mergeAll(providerLayer, directoryLayer)));

      assert.equal(codex.stopSession.mock.calls.length, 1);
      assert.equal(claude.stopSession.mock.calls.length, 0);
    }),
);

routing.layer("ProviderServiceLive routing", (it) => {
  it.effect("serializes lifecycle mutations and persists a fresh generation per start", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService;
      const directory = yield* ProviderSessionDirectory;
      const threadId = asThreadId("thread-lifecycle-generation");
      const startInput: ProviderSessionStartInput = {
        provider: "codex",
        threadId,
        cwd: "/tmp/project",
        runtimeMode: "full-access",
      };

      yield* provider.startSession(threadId, startInput);
      const firstBinding = Option.getOrUndefined(yield* directory.getBinding(threadId));
      const firstGeneration = firstBinding?.lifecycleGeneration;
      assert.equal(typeof firstGeneration, "string");

      yield* provider.stopSession({ threadId });
      yield* provider.startSession(threadId, startInput);
      const secondBinding = Option.getOrUndefined(yield* directory.getBinding(threadId));
      const secondGeneration = secondBinding?.lifecycleGeneration;
      assert.equal(typeof secondGeneration, "string");
      assert.notEqual(secondGeneration, firstGeneration);

      const responseCallCount = routing.codex.respondToRequest.mock.calls.length;
      const staleResponse = yield* Effect.result(
        provider.respondToRequest({
          threadId,
          requestId: asRequestId("request-from-old-generation"),
          lifecycleGeneration: String(firstGeneration),
          decision: "accept",
        }),
      );
      assertFailure(
        staleResponse,
        new ProviderValidationError({
          operation: "ProviderService.respondToRequest",
          issue: `Cannot respond to stale request 'request-from-old-generation' from provider generation '${String(firstGeneration)}'.`,
        }),
      );
      assert.equal(routing.codex.respondToRequest.mock.calls.length, responseCallCount);

      const userInputResponseCallCount = routing.codex.respondToUserInput.mock.calls.length;
      const staleUserInputResponse = yield* Effect.result(
        provider.respondToUserInput({
          threadId,
          requestId: asRequestId("user-input-from-old-generation"),
          lifecycleGeneration: String(firstGeneration),
          answers: { answer: "stale" },
        }),
      );
      assertFailure(
        staleUserInputResponse,
        new ProviderValidationError({
          operation: "ProviderService.respondToUserInput",
          issue: `Cannot respond to stale request 'user-input-from-old-generation' from provider generation '${String(firstGeneration)}'.`,
        }),
      );
      assert.equal(routing.codex.respondToUserInput.mock.calls.length, userInputResponseCallCount);

      yield* routing.codex.waitForRuntimeSubscribers();
      routing.codex.emit({
        type: "session.exited",
        eventId: asEventId("runtime-old-generation-exited"),
        provider: "codex",
        threadId,
        createdAt: "2026-07-14T14:00:00.000Z",
        lifecycleGeneration: String(firstGeneration),
        payload: { reason: "late old-runtime exit" },
      });
      yield* sleep(25);
      const bindingAfterStaleEvent = Option.getOrUndefined(yield* directory.getBinding(threadId));
      assert.equal(bindingAfterStaleEvent?.lifecycleGeneration, secondGeneration);
      assert.equal(bindingAfterStaleEvent?.status, "running");

      const defaultStart = routing.codex.startSession.getMockImplementation();
      if (!defaultStart) assert.fail("Expected the fake adapter start implementation");
      let releaseDelayedStart: () => void = () => undefined;
      const delayedStart = new Promise<void>((resolve) => {
        releaseDelayedStart = resolve;
      });
      routing.codex.startSession.mockImplementationOnce((input) =>
        Effect.promise(() => delayedStart).pipe(Effect.andThen(defaultStart(input))),
      );
      const startCallCount = routing.codex.startSession.mock.calls.length;
      const stopCallCount = routing.codex.stopSession.mock.calls.length;
      const startFiber = yield* provider.startSession(threadId, startInput).pipe(Effect.forkChild);
      yield* waitUntil(
        () => routing.codex.startSession.mock.calls.length > startCallCount,
        500,
        10,
        "delayed provider start",
      );
      const stopFiber = yield* provider.stopSession({ threadId }).pipe(Effect.forkChild);
      yield* sleep(25);
      assert.equal(routing.codex.stopSession.mock.calls.length, stopCallCount);

      releaseDelayedStart();
      yield* Fiber.join(startFiber);
      yield* Fiber.join(stopFiber);
      assert.equal(Option.isNone(yield* directory.getBinding(threadId)), true);
    }),
  );

  it.effect("serializes overlapping starts and rejects incompatible provider continuation", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService;
      const directory = yield* ProviderSessionDirectory;
      const threadId = asThreadId("thread-overlapping-provider-starts");
      const codexInput: ProviderSessionStartInput = {
        provider: "codex",
        threadId,
        cwd: "/tmp/provider-starts",
        runtimeMode: "full-access",
      };

      yield* provider.startSession(threadId, codexInput);
      const defaultCodexStart = routing.codex.startSession.getMockImplementation();
      if (!defaultCodexStart) assert.fail("Expected the fake Codex start implementation");

      let releaseSameProviderStart: () => void = () => undefined;
      const delayedSameProviderStart = new Promise<void>((resolve) => {
        releaseSameProviderStart = resolve;
      });
      routing.codex.startSession.mockImplementationOnce((input) =>
        Effect.promise(() => delayedSameProviderStart).pipe(
          Effect.andThen(defaultCodexStart(input)),
        ),
      );
      const codexStartCount = routing.codex.startSession.mock.calls.length;
      const claudeStartCount = routing.claude.startSession.mock.calls.length;

      const sameProviderFiber = yield* provider
        .startSession(threadId, codexInput)
        .pipe(Effect.forkChild);
      yield* waitUntil(
        () => routing.codex.startSession.mock.calls.length > codexStartCount,
        500,
        10,
        "same-provider start",
      );
      const crossProviderFiber = yield* Effect.result(
        provider.startSession(threadId, {
            provider: "claudeAgent",
            threadId,
            cwd: "/tmp/provider-starts",
            runtimeMode: "full-access",
          }),
        )
        .pipe(Effect.forkChild);
      yield* sleep(25);
      assert.equal(routing.claude.startSession.mock.calls.length, claudeStartCount);

      releaseSameProviderStart();
      yield* Fiber.join(sameProviderFiber);
      const crossProviderResult = yield* Fiber.join(crossProviderFiber);
      assertFailure(
        crossProviderResult,
        new ProviderValidationError({
          operation: "ProviderService.startSession",
          issue: `Cannot continue thread '${threadId}' from provider instance 'codex' (codex) with 'claudeAgent' (claudeAgent) because their native session storage is incompatible. Start a new thread or restore the original provider home.`,
        }),
      );

      const binding = Option.getOrUndefined(yield* directory.getBinding(threadId));
      const [codexSessions, claudeSessions] = yield* Effect.all([
        routing.codex.listSessions(),
        routing.claude.listSessions(),
      ]);
      assert.equal(binding?.provider, "codex");
      assert.equal(
        codexSessions.some((session) => session.threadId === threadId),
        true,
      );
      assert.equal(claudeSessions.filter((session) => session.threadId === threadId).length, 0);

      yield* provider.stopSession({ threadId });
    }),
  );

  it.effect("preserves the previous runtime when incompatible replacement is rejected", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService;
      const directory = yield* ProviderSessionDirectory;
      const threadId = asThreadId("thread-failed-provider-replacement");
      const initial = yield* provider.startSession(threadId, {
        provider: "codex",
        threadId,
        cwd: "/tmp/failed-provider-replacement",
        runtimeMode: "full-access",
      });
      const originalBinding = Option.getOrUndefined(yield* directory.getBinding(threadId));
      const replacement = yield* Effect.result(
        provider.startSession(threadId, {
          provider: "claudeAgent",
          threadId,
          cwd: "/tmp/failed-provider-replacement",
          runtimeMode: "full-access",
        }),
      );
      assertFailure(
        replacement,
        new ProviderValidationError({
          operation: "ProviderService.startSession",
          issue: `Cannot continue thread '${threadId}' from provider instance 'codex' (codex) with 'claudeAgent' (claudeAgent) because their native session storage is incompatible. Start a new thread or restore the original provider home.`,
        }),
      );

      const restoredBinding = Option.getOrUndefined(yield* directory.getBinding(threadId));
      const [codexSessions, claudeSessions] = yield* Effect.all([
        routing.codex.listSessions(),
        routing.claude.listSessions(),
      ]);
      assert.equal(restoredBinding?.provider, "codex");
      assert.equal(restoredBinding?.status, "running");
      assert.equal(restoredBinding?.lifecycleGeneration, originalBinding?.lifecycleGeneration);
      assert.equal(codexSessions.filter((session) => session.threadId === threadId).length, 1);
      assert.equal(
        claudeSessions.some((session) => session.threadId === threadId),
        false,
      );
      assert.deepEqual(restoredBinding?.resumeCursor, initial.resumeCursor);

      yield* provider.stopSession({ threadId });
    }),
  );

  it.effect("serializes recovery before rejecting a competing provider start", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService;
      const directory = yield* ProviderSessionDirectory;
      const threadId = asThreadId("thread-recovery-start-race");
      const initial = yield* provider.startSession(threadId, {
        provider: "codex",
        threadId,
        cwd: "/tmp/recovery-start-race",
        runtimeMode: "full-access",
      });
      assert.equal(typeof provider.stopRuntimeSession, "function");
      if (!provider.stopRuntimeSession) assert.fail("Expected stopRuntimeSession");
      yield* provider.stopRuntimeSession({ threadId });

      const defaultCodexStart = routing.codex.startSession.getMockImplementation();
      if (!defaultCodexStart) assert.fail("Expected the fake Codex start implementation");
      let releaseRecovery: () => void = () => undefined;
      const delayedRecovery = new Promise<void>((resolve) => {
        releaseRecovery = resolve;
      });
      routing.codex.startSession.mockImplementationOnce((input) =>
        Effect.promise(() => delayedRecovery).pipe(Effect.andThen(defaultCodexStart(input))),
      );
      const codexStartCount = routing.codex.startSession.mock.calls.length;
      const claudeStartCount = routing.claude.startSession.mock.calls.length;

      const recoveryFiber = yield* provider
        .sendTurn({ threadId, input: "recover", attachments: [] })
        .pipe(Effect.forkChild);
      yield* waitUntil(
        () => routing.codex.startSession.mock.calls.length > codexStartCount,
        500,
        10,
        "provider recovery start",
      );
      const competingStartFiber = yield* Effect.result(
        provider.startSession(threadId, {
            provider: "claudeAgent",
            threadId,
            cwd: "/tmp/recovery-start-race",
            runtimeMode: "full-access",
          }),
        )
        .pipe(Effect.forkChild);
      yield* sleep(25);
      assert.equal(routing.claude.startSession.mock.calls.length, claudeStartCount);

      releaseRecovery();
      yield* Fiber.join(recoveryFiber);
      const competingStartResult = yield* Fiber.join(competingStartFiber);
      assertFailure(
        competingStartResult,
        new ProviderValidationError({
          operation: "ProviderService.startSession",
          issue: `Cannot continue thread '${threadId}' from provider instance 'codex' (codex) with 'claudeAgent' (claudeAgent) because their native session storage is incompatible. Start a new thread or restore the original provider home.`,
        }),
      );

      const binding = Option.getOrUndefined(yield* directory.getBinding(threadId));
      const [codexSessions, claudeSessions] = yield* Effect.all([
        routing.codex.listSessions(),
        routing.claude.listSessions(),
      ]);
      const recoveryCall = routing.codex.startSession.mock.calls.findLast(
        ([input]) => input.threadId === threadId,
      )?.[0];
      assert.equal(binding?.provider, "codex");
      assert.equal(
        codexSessions.some((session) => session.threadId === threadId),
        true,
      );
      assert.equal(claudeSessions.filter((session) => session.threadId === threadId).length, 0);
      assert.deepEqual(recoveryCall?.resumeCursor, initial.resumeCursor);

      yield* provider.stopSession({ threadId });
    }),
  );

  it.effect("requires the source lifecycle generation for modern Claude interactions", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService;
      const directory = yield* ProviderSessionDirectory;
      const threadId = asThreadId("thread-claude-interaction-generation");

      yield* provider.startSession(threadId, {
        provider: "claudeAgent",
        threadId,
        cwd: "/tmp/project",
        runtimeMode: "approval-required",
      });
      const binding = Option.getOrUndefined(yield* directory.getBinding(threadId));
      const lifecycleGeneration = binding?.lifecycleGeneration;
      assert.equal(typeof lifecycleGeneration, "string");

      const approvalCallCount = routing.claude.respondToRequest.mock.calls.length;
      const missingApprovalGeneration = yield* Effect.result(
        provider.respondToRequest({
          threadId,
          requestId: asRequestId("claude-approval-without-generation"),
          decision: "accept",
        }),
      );
      assertFailure(
        missingApprovalGeneration,
        new ProviderValidationError({
          operation: "ProviderService.respondToRequest",
          issue:
            "Cannot respond to request 'claude-approval-without-generation' without its provider lifecycle generation.",
        }),
      );
      assert.equal(routing.claude.respondToRequest.mock.calls.length, approvalCallCount);

      const userInputCallCount = routing.claude.respondToUserInput.mock.calls.length;
      const missingUserInputGeneration = yield* Effect.result(
        provider.respondToUserInput({
          threadId,
          requestId: asRequestId("claude-user-input-without-generation"),
          answers: { answer: "continue" },
        }),
      );
      assertFailure(
        missingUserInputGeneration,
        new ProviderValidationError({
          operation: "ProviderService.respondToUserInput",
          issue:
            "Cannot respond to request 'claude-user-input-without-generation' without its provider lifecycle generation.",
        }),
      );
      assert.equal(routing.claude.respondToUserInput.mock.calls.length, userInputCallCount);

      yield* provider.respondToRequest({
        threadId,
        requestId: asRequestId("claude-approval-current-generation"),
        lifecycleGeneration,
        decision: "accept",
      });
      yield* provider.respondToUserInput({
        threadId,
        requestId: asRequestId("claude-user-input-current-generation"),
        lifecycleGeneration,
        answers: { answer: "continue" },
      });
      assert.equal(routing.claude.respondToRequest.mock.calls.length, approvalCallCount + 1);
      assert.equal(routing.claude.respondToUserInput.mock.calls.length, userInputCallCount + 1);
      yield* provider.stopSession({ threadId });
      routing.claude.startSession.mockClear();
      routing.claude.respondToRequest.mockClear();
      routing.claude.respondToUserInput.mockClear();
      routing.claude.stopSession.mockClear();
    }),
  );

  it.effect("requires the source lifecycle generation for modern Antigravity approvals", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService;
      const directory = yield* ProviderSessionDirectory;
      const threadId = asThreadId("thread-antigravity-interaction-generation");

      yield* provider.startSession(threadId, {
        provider: "antigravity",
        threadId,
        cwd: "/tmp/project",
        runtimeMode: "approval-required",
      });
      const binding = Option.getOrUndefined(yield* directory.getBinding(threadId));
      const lifecycleGeneration = binding?.lifecycleGeneration;
      assert.equal(typeof lifecycleGeneration, "string");

      const responseCallCount = routing.antigravity.respondToRequest.mock.calls.length;
      const missingGeneration = yield* Effect.result(
        provider.respondToRequest({
          threadId,
          requestId: asRequestId("antigravity-approval-without-generation"),
          decision: "accept",
        }),
      );
      assertFailure(
        missingGeneration,
        new ProviderValidationError({
          operation: "ProviderService.respondToRequest",
          issue:
            "Cannot respond to request 'antigravity-approval-without-generation' without its provider lifecycle generation.",
        }),
      );
      assert.equal(routing.antigravity.respondToRequest.mock.calls.length, responseCallCount);

      yield* provider.respondToRequest({
        threadId,
        requestId: asRequestId("antigravity-approval-current-generation"),
        lifecycleGeneration,
        decision: "accept",
      });
      assert.equal(routing.antigravity.respondToRequest.mock.calls.length, responseCallCount + 1);

      yield* provider.stopSession({ threadId });
      routing.antigravity.startSession.mockClear();
      routing.antigravity.respondToRequest.mockClear();
      routing.antigravity.stopSession.mockClear();
    }),
  );

  it.effect("routes provider operations and rollback conversation", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService;
      const directory = yield* ProviderSessionDirectory;
      routing.codex.sendTurn.mockClear();
      routing.codex.interruptTurn.mockClear();
      routing.codex.respondToRequest.mockClear();
      routing.codex.respondToUserInput.mockClear();

      const session = yield* provider.startSession(asThreadId("thread-1"), {
        provider: "codex",
        threadId: asThreadId("thread-1"),
        cwd: "/tmp/project",
        runtimeMode: "full-access",
      });
      assert.equal(session.provider, "codex");
      const binding = Option.getOrUndefined(yield* directory.getBinding(session.threadId));
      const lifecycleGeneration = binding?.lifecycleGeneration;
      assert.equal(typeof lifecycleGeneration, "string");

      const sessions = yield* provider.listSessions();
      assert.equal(sessions.length, 1);

      yield* provider.sendTurn({
        threadId: session.threadId,
        input: "hello",
        attachments: [],
      });
      assert.equal(routing.codex.sendTurn.mock.calls.length, 1);

      yield* provider.interruptTurn({ threadId: session.threadId });
      assert.deepEqual(routing.codex.interruptTurn.mock.calls, [
        [session.threadId, asTurnId("turn-thread-1"), undefined],
      ]);

      yield* provider.respondToRequest({
        threadId: session.threadId,
        requestId: asRequestId("req-1"),
        lifecycleGeneration,
        decision: "accept",
      });
      assert.deepEqual(routing.codex.respondToRequest.mock.calls, [
        [session.threadId, asRequestId("req-1"), "accept"],
      ]);

      yield* provider.respondToUserInput({
        threadId: session.threadId,
        requestId: asRequestId("req-user-input-1"),
        lifecycleGeneration,
        answers: {
          sandbox_mode: "workspace-write",
        },
      });
      assert.deepEqual(routing.codex.respondToUserInput.mock.calls, [
        [
          session.threadId,
          asRequestId("req-user-input-1"),
          {
            sandbox_mode: "workspace-write",
          },
        ],
      ]);

      yield* provider.rollbackConversation({
        threadId: session.threadId,
        numTurns: 0,
      });

      yield* provider.stopSession({ threadId: session.threadId });
      const sendAfterStop = yield* Effect.result(
        provider.sendTurn({
          threadId: session.threadId,
          input: "after-stop",
          attachments: [],
        }),
      );
      assertFailure(
        sendAfterStop,
        new ProviderValidationError({
          operation: "ProviderService.sendTurn",
          issue: `Cannot route thread '${session.threadId}' because no persisted provider binding exists.`,
        }),
      );
    }),
  );

  it.effect("rejects provider and provider-instance driver mismatches", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService;
      routing.codex.startSession.mockClear();
      routing.claude.startSession.mockClear();

      const result = yield* Effect.result(
        provider.startSession(asThreadId("thread-mismatch"), {
          provider: "codex",
          providerInstanceId: "claudeAgent",
          threadId: asThreadId("thread-mismatch"),
          runtimeMode: "full-access",
        }),
      );

      assertFailure(
        result,
        new ProviderValidationError({
          operation: "ProviderService.startSession",
          issue: "Unknown provider instance 'claudeAgent'.",
        }),
      );
      assert.equal(routing.codex.startSession.mock.calls.length, 0);
      assert.equal(routing.claude.startSession.mock.calls.length, 0);
    }),
  );

  it.effect("rejects a provider switch that would discard native continuation", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService;
      const serverSettings = yield* ServerSettingsService;
      const threadId = asThreadId("thread-cross-provider-default-instance");

      yield* serverSettings.updateSettings({
        providerInstances: {
          claude_work: {
            driver: "claudeAgent",
            enabled: true,
          },
        },
      });
      yield* provider.startSession(threadId, {
        provider: "claudeAgent",
        providerInstanceId: "claude_work",
        threadId,
        runtimeMode: "full-access",
      });
      routing.codex.startSession.mockClear();

      const result = yield* Effect.result(
        provider.startSession(threadId, {
          provider: "codex",
          threadId,
          runtimeMode: "full-access",
        }),
      );

      assert.equal(result._tag, "Failure");
      if (result._tag === "Failure") {
        assert.match(String(result.failure), /native session storage is incompatible/);
      }
      assert.equal(routing.codex.startSession.mock.calls.length, 0);
    }),
  );

  it.effect("prepares a newly selected Codex account before reusing shared continuation", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService;
      const threadId = asThreadId("thread-codex-shared-continuation-home");
      const fixture = makeSharedCodexContinuationFixture(["personal", "work"], ["personal"]);
      const initial = yield* provider.startSession(threadId, {
        provider: "codex",
        threadId,
        providerOptions: {
          codex: {
            homePath: fixture.homePath,
            shadowHomePath: fixture.shadowHomePath("personal"),
            accountId: "personal",
            environment: fixture.environment,
          },
        },
        runtimeMode: "full-access",
      });
      routing.codex.startSession.mockClear();
      routing.codex.stopSession.mockClear();
      assert.equal(
        isCodexSharedContinuationStatePrepared({
          env: { ...process.env, ...fixture.environment },
          homePath: fixture.homePath,
          shadowHomePath: fixture.shadowHomePath("work"),
          accountId: "work",
        }),
        false,
      );

      yield* provider.startSession(threadId, {
        provider: "codex",
        threadId,
        providerOptions: {
          codex: {
            homePath: fixture.homePath,
            shadowHomePath: fixture.shadowHomePath("work"),
            accountId: "work",
            environment: fixture.environment,
          },
        },
        runtimeMode: "full-access",
      });

      assert.equal(routing.codex.stopSession.mock.calls.length, 0);
      assert.equal(routing.codex.startSession.mock.calls.length, 1);
      assert.equal(
        isCodexSharedContinuationStatePrepared({
          env: { ...process.env, ...fixture.environment },
          homePath: fixture.homePath,
          shadowHomePath: fixture.shadowHomePath("work"),
          accountId: "work",
        }),
        true,
      );
      assert.deepEqual(
        routing.codex.startSession.mock.calls[0]?.[0].resumeCursor,
        initial.resumeCursor,
      );
      assert.equal(
        routing.codex.startSession.mock.calls[0]?.[0].expectedCodexContinuationGeneration,
        readCodexSharedContinuationGeneration({
          env: { ...process.env, ...fixture.environment },
          homePath: fixture.homePath,
          shadowHomePath: fixture.shadowHomePath("work"),
          accountId: "work",
        }),
      );
      fs.rmSync(fixture.root, { recursive: true, force: true });
    }),
  );

  it.effect("pins an imported explicit Codex resume through the adapter launch seam", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService;
      const threadId = asThreadId("thread-codex-import-explicit-resume");
      const fixture = makeSharedCodexContinuationFixture(["personal"], ["personal"]);
      const providerOptions = {
        codex: {
          homePath: fixture.homePath,
          shadowHomePath: fixture.shadowHomePath("personal"),
          accountId: "personal",
          environment: fixture.environment,
        },
      };
      const overlayHomePath = resolveActiveCodexHomeWritePath({
        env: { ...process.env, ...fixture.environment },
        ...providerOptions.codex,
      });
      const generationBeforePreflight = readCodexSharedContinuationGeneration({
        env: { ...process.env, ...fixture.environment },
        ...providerOptions.codex,
      });
      let launchError: unknown;
      routing.codex.startSession.mockClear();
      routing.codex.setBeforeStartSession((input) => {
        fs.rmSync(fixture.homePath, { recursive: true, force: true });
        fs.rmSync(overlayHomePath, { recursive: true, force: true });
        try {
          buildCodexProcessLaunchContext({
            env: { ...process.env, ...fixture.environment },
            homePath: fixture.homePath,
            shadowHomePath: fixture.shadowHomePath("personal"),
            accountId: "personal",
            ...(input.expectedCodexContinuationGeneration
              ? {
                  expectedSharedContinuationGeneration: input.expectedCodexContinuationGeneration,
                }
              : {}),
          });
        } catch (error) {
          launchError = error;
        }
      });

      try {
        yield* provider.startSession(threadId, {
          provider: "codex",
          threadId,
          providerOptions,
          resumeCursor: { threadId: "imported-provider-thread" },
          runtimeMode: "full-access",
        });

        const launchInput = routing.codex.startSession.mock.calls[0]?.[0];
        assert.equal(launchInput?.expectedCodexContinuationGeneration, generationBeforePreflight);
        assert.match(String(launchError), /missing or damaged|refusing to recreate/);
        assert.equal(fs.existsSync(fixture.homePath), false);
        assert.equal(fs.existsSync(overlayHomePath), false);
      } finally {
        routing.codex.setBeforeStartSession(undefined);
        fs.rmSync(fixture.root, { recursive: true, force: true });
      }
    }),
  );

  it.effect("does not recreate damaged shared source state during a direct account switch", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService;
      const threadId = asThreadId("thread-codex-switch-damaged-source");
      const fixture = makeSharedCodexContinuationFixture(["personal", "work"], ["personal"]);
      const options = (accountId: "personal" | "work") => ({
        codex: {
          homePath: fixture.homePath,
          shadowHomePath: fixture.shadowHomePath(accountId),
          accountId,
          environment: fixture.environment,
        },
      });
      yield* provider.startSession(threadId, {
        provider: "codex",
        threadId,
        providerOptions: options("personal"),
        runtimeMode: "full-access",
      });
      const workOverlayHomePath = resolveActiveCodexHomeWritePath({
        env: { ...process.env, ...fixture.environment },
        homePath: fixture.homePath,
        shadowHomePath: fixture.shadowHomePath("work"),
        accountId: "work",
      });
      assert.equal(fs.existsSync(workOverlayHomePath), false);
      const sourceSessionsPath = path.join(fixture.homePath, "sessions");
      fs.rmSync(sourceSessionsPath, { recursive: true, force: true });
      routing.codex.startSession.mockClear();
      routing.codex.stopSession.mockClear();

      const result = yield* Effect.result(
        provider.startSession(threadId, {
          provider: "codex",
          threadId,
          providerOptions: options("work"),
          runtimeMode: "full-access",
        }),
      );

      assert.equal(result._tag, "Failure");
      if (result._tag === "Failure") {
        assert.match(String(result.failure), /missing (?:or damaged|'sessions')/);
      }
      assert.equal(routing.codex.startSession.mock.calls.length, 0);
      assert.equal(routing.codex.stopSession.mock.calls.length, 0);
      assert.equal(fs.existsSync(sourceSessionsPath), false);
      assert.equal(fs.existsSync(workOverlayHomePath), false);
      fs.rmSync(fixture.root, { recursive: true, force: true });
    }),
  );

  it.effect("prepares a newly configured Codex account during persisted recovery", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService;
      const serverSettings = yield* ServerSettingsService;
      const threadId = asThreadId("thread-codex-recovery-prepares-new-account");
      const fixture = makeSharedCodexContinuationFixture(["personal", "work"], ["personal"]);
      const instance = (accountId: "personal" | "work") => ({
        driver: "codex" as const,
        environment: fixture.instanceEnvironment,
        config: {
          homePath: fixture.homePath,
          shadowHomePath: fixture.shadowHomePath(accountId),
          accountId,
        },
      });
      yield* serverSettings.updateSettings({
        providerInstances: { codex_selected: instance("personal") },
      });
      const initial = yield* provider.startSession(threadId, {
        provider: "codex",
        providerInstanceId: "codex_selected",
        threadId,
        runtimeMode: "full-access",
      });
      yield* routing.codex.stopAll();
      yield* serverSettings.updateSettings({
        providerInstances: { codex_selected: instance("work") },
      });
      assert.equal(
        isCodexSharedContinuationStatePrepared({
          env: { ...process.env, ...fixture.environment },
          homePath: fixture.homePath,
          shadowHomePath: fixture.shadowHomePath("work"),
          accountId: "work",
        }),
        false,
      );
      routing.codex.startSession.mockClear();
      routing.codex.sendTurn.mockClear();

      yield* provider.sendTurn({
        threadId,
        input: "resume on newly configured work account",
        attachments: [],
      });

      assert.equal(
        isCodexSharedContinuationStatePrepared({
          env: { ...process.env, ...fixture.environment },
          homePath: fixture.homePath,
          shadowHomePath: fixture.shadowHomePath("work"),
          accountId: "work",
        }),
        true,
      );
      assert.equal(routing.codex.startSession.mock.calls.length, 1);
      assert.deepEqual(
        routing.codex.startSession.mock.calls[0]?.[0].resumeCursor,
        initial.resumeCursor,
      );
      assert.equal(routing.codex.sendTurn.mock.calls.length, 1);
      fs.rmSync(fixture.root, { recursive: true, force: true });
    }),
  );

  it.effect("does not recreate a deleted shared source home during persisted recovery", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService;
      const serverSettings = yield* ServerSettingsService;
      const threadId = asThreadId("thread-codex-recovery-deleted-source");
      const fixture = makeSharedCodexContinuationFixture(["personal", "work"], ["personal"]);
      const instance = (accountId: "personal" | "work") => ({
        driver: "codex" as const,
        environment: fixture.instanceEnvironment,
        config: {
          homePath: fixture.homePath,
          shadowHomePath: fixture.shadowHomePath(accountId),
          accountId,
        },
      });
      yield* serverSettings.updateSettings({
        providerInstances: { codex_selected: instance("personal") },
      });
      yield* provider.startSession(threadId, {
        provider: "codex",
        providerInstanceId: "codex_selected",
        threadId,
        runtimeMode: "full-access",
      });
      const workOverlayHomePath = resolveActiveCodexHomeWritePath({
        env: { ...process.env, ...fixture.environment },
        homePath: fixture.homePath,
        shadowHomePath: fixture.shadowHomePath("work"),
        accountId: "work",
      });
      assert.equal(fs.existsSync(workOverlayHomePath), false);
      yield* routing.codex.stopAll();
      yield* serverSettings.updateSettings({
        providerInstances: { codex_selected: instance("work") },
      });
      fs.rmSync(fixture.homePath, { recursive: true, force: true });
      routing.codex.startSession.mockClear();
      routing.codex.sendTurn.mockClear();

      const result = yield* Effect.result(
        provider.sendTurn({
          threadId,
          input: "resume after source deletion",
          attachments: [],
        }),
      );

      assert.equal(result._tag, "Failure");
      if (result._tag === "Failure") {
        assert.match(String(result.failure), /missing or damaged/);
      }
      assert.equal(routing.codex.startSession.mock.calls.length, 0);
      assert.equal(routing.codex.sendTurn.mock.calls.length, 0);
      assert.equal(fs.existsSync(fixture.homePath), false);
      assert.equal(fs.existsSync(workOverlayHomePath), false);
      fs.rmSync(fixture.root, { recursive: true, force: true });
    }),
  );

  it.effect("resumes account A after an unprepared conflicting account B is rejected", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService;
      const threadId = asThreadId("thread-codex-stale-shared-marker");
      const fixture = makeSharedCodexContinuationFixture(["personal", "work"], ["personal"]);
      const personalOptions = {
        codex: {
          homePath: fixture.homePath,
          shadowHomePath: fixture.shadowHomePath("personal"),
          accountId: "personal",
          environment: fixture.environment,
        },
      };
      const workOptions = {
        codex: {
          homePath: fixture.homePath,
          shadowHomePath: fixture.shadowHomePath("work"),
          accountId: "work",
          environment: fixture.environment,
        },
      };
      const initial = yield* provider.startSession(threadId, {
        provider: "codex",
        threadId,
        providerOptions: personalOptions,
        runtimeMode: "full-access",
      });
      assert.equal(typeof provider.stopRuntimeSession, "function");
      if (!provider.stopRuntimeSession) return;
      yield* provider.stopRuntimeSession({ threadId });

      const workOverlayHomePath = resolveActiveCodexHomeWritePath({
        env: { ...process.env, ...fixture.environment },
        homePath: fixture.homePath,
        shadowHomePath: fixture.shadowHomePath("work"),
        accountId: "work",
      });
      fs.mkdirSync(path.join(workOverlayHomePath, "sessions"), { recursive: true });
      fs.writeFileSync(
        path.join(workOverlayHomePath, "sessions", "independent.jsonl"),
        "independent",
        "utf8",
      );
      assert.equal(
        isCodexSharedContinuationStatePrepared({
          env: { ...process.env, ...fixture.environment },
          homePath: fixture.homePath,
          shadowHomePath: fixture.shadowHomePath("work"),
          accountId: "work",
        }),
        false,
      );
      routing.codex.startSession.mockClear();

      const result = yield* Effect.result(
        provider.startSession(threadId, {
          provider: "codex",
          threadId,
          providerOptions: workOptions,
          runtimeMode: "full-access",
        }),
      );
      assert.equal(result._tag, "Failure");
      if (result._tag === "Failure") {
        assert.match(String(result.failure), /refusing to overwrite raced or independently owned/);
      }
      assert.equal(routing.codex.startSession.mock.calls.length, 0);
      assert.strictEqual(
        fs.readFileSync(path.join(workOverlayHomePath, "sessions", "independent.jsonl"), "utf8"),
        "independent",
      );
      yield* provider.startSession(threadId, {
        provider: "codex",
        threadId,
        providerOptions: personalOptions,
        runtimeMode: "full-access",
      });
      assert.equal(routing.codex.startSession.mock.calls.length, 1);
      assert.deepEqual(
        routing.codex.startSession.mock.calls[0]?.[0].resumeCursor,
        initial.resumeCursor,
      );
      fs.rmSync(fixture.root, { recursive: true, force: true });
    }),
  );

  it.effect("does not let an exact Codex launch downgrade a persisted shared identity", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService;
      const threadId = asThreadId("thread-codex-stale-shared-marker-exact-launch");
      const fixture = makeSharedCodexContinuationFixture(["personal"]);
      const personalOptions = {
        codex: {
          homePath: fixture.homePath,
          shadowHomePath: fixture.shadowHomePath("personal"),
          accountId: "personal",
          environment: fixture.environment,
        },
      };
      yield* provider.startSession(threadId, {
        provider: "codex",
        threadId,
        providerOptions: personalOptions,
        runtimeMode: "full-access",
      });

      const personalOverlayHomePath = resolveActiveCodexHomeWritePath({
        env: { ...process.env, ...fixture.environment },
        homePath: fixture.homePath,
        shadowHomePath: fixture.shadowHomePath("personal"),
        accountId: "personal",
      });
      fs.unlinkSync(path.join(personalOverlayHomePath, "sessions"));
      fs.mkdirSync(path.join(personalOverlayHomePath, "sessions"));
      fs.writeFileSync(
        path.join(personalOverlayHomePath, "sessions", "independent.jsonl"),
        "independent",
        "utf8",
      );
      routing.codex.startSession.mockClear();

      const result = yield* Effect.result(
        provider.startSession(threadId, {
          provider: "codex",
          threadId,
          providerOptions: personalOptions,
          runtimeMode: "full-access",
        }),
      );
      assert.equal(result._tag, "Failure");
      if (result._tag === "Failure") {
        assert.match(String(result.failure), /refusing to overwrite raced or independently owned/);
      }
      assert.equal(routing.codex.startSession.mock.calls.length, 0);
      assert.strictEqual(
        fs.readFileSync(
          path.join(personalOverlayHomePath, "sessions", "independent.jsonl"),
          "utf8",
        ),
        "independent",
      );
      fs.rmSync(fixture.root, { recursive: true, force: true });
    }),
  );

  it.effect("reuses stopped Codex continuation when switching instances on a shared home", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService;
      const serverSettings = yield* ServerSettingsService;
      const threadId = asThreadId("thread-codex-stopped-shared-home");
      const fixture = makeSharedCodexContinuationFixture(["personal", "work"]);
      yield* serverSettings.updateSettings({
        providerInstances: {
          codex_personal: {
            driver: "codex",
            environment: fixture.instanceEnvironment,
            config: {
              homePath: fixture.homePath,
              shadowHomePath: fixture.shadowHomePath("personal"),
              accountId: "personal",
            },
          },
          codex_work: {
            driver: "codex",
            environment: fixture.instanceEnvironment,
            config: {
              homePath: fixture.homePath,
              shadowHomePath: fixture.shadowHomePath("work"),
              accountId: "work",
            },
          },
        },
      });
      const initial = yield* provider.startSession(threadId, {
        provider: "codex",
        providerInstanceId: "codex_personal",
        threadId,
        runtimeMode: "full-access",
      });
      assert.equal(typeof provider.stopRuntimeSession, "function");
      if (!provider.stopRuntimeSession) return;
      yield* provider.stopRuntimeSession({ threadId });
      routing.codex.startSession.mockClear();

      yield* provider.startSession(threadId, {
        provider: "codex",
        providerInstanceId: "codex_work",
        threadId,
        runtimeMode: "full-access",
      });

      assert.equal(routing.codex.startSession.mock.calls.length, 1);
      assert.equal(routing.codex.startSession.mock.calls[0]?.[0].providerInstanceId, "codex_work");
      assert.deepEqual(
        routing.codex.startSession.mock.calls[0]?.[0].resumeCursor,
        initial.resumeCursor,
      );
      fs.rmSync(fixture.root, { recursive: true, force: true });
    }),
  );

  it.effect("rejects incompatible live Claude home changes before stopping the runtime", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService;
      const directory = yield* ProviderSessionDirectory;
      const threadId = asThreadId("thread-claude-live-home-boundary");
      yield* provider.startSession(threadId, {
        provider: "claudeAgent",
        threadId,
        providerOptions: { claudeAgent: { homePath: "/tmp/claude-home-a" } },
        runtimeMode: "full-access",
      });
      const binding = Option.getOrUndefined(yield* directory.getBinding(threadId));
      assert.ok(binding);
      yield* directory.upsert({ ...binding, resumeCursor: null });
      routing.claude.updateSession(threadId, (session) => {
        const { resumeCursor: _resumeCursor, ...withoutResumeCursor } = session;
        return withoutResumeCursor;
      });
      routing.claude.startSession.mockClear();
      routing.claude.stopSession.mockClear();

      const result = yield* Effect.result(
        provider.startSession(threadId, {
          provider: "claudeAgent",
          threadId,
          providerOptions: { claudeAgent: { homePath: "/tmp/claude-home-b" } },
          runtimeMode: "full-access",
        }),
      );

      assert.equal(result._tag, "Failure");
      assert.equal(routing.claude.stopSession.mock.calls.length, 0);
      assert.equal(routing.claude.startSession.mock.calls.length, 0);
    }),
  );

  it.effect("rejects incompatible stopped Claude home changes", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService;
      const threadId = asThreadId("thread-claude-stopped-home-boundary");
      yield* provider.startSession(threadId, {
        provider: "claudeAgent",
        threadId,
        providerOptions: { claudeAgent: { homePath: "/tmp/claude-stopped-home-a" } },
        runtimeMode: "full-access",
      });
      assert.equal(typeof provider.stopRuntimeSession, "function");
      if (!provider.stopRuntimeSession) return;
      yield* provider.stopRuntimeSession({ threadId });
      routing.claude.startSession.mockClear();

      const result = yield* Effect.result(
        provider.startSession(threadId, {
          provider: "claudeAgent",
          threadId,
          providerOptions: { claudeAgent: { homePath: "/tmp/claude-stopped-home-b" } },
          runtimeMode: "full-access",
        }),
      );

      assert.equal(result._tag, "Failure");
      assert.equal(routing.claude.startSession.mock.calls.length, 0);
    }),
  );

  it.effect("rejects stopped Claude continuation after credentials change at the same home", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService;
      const serverSettings = yield* ServerSettingsService;
      const threadId = asThreadId("thread-claude-stopped-credential-boundary");
      const providerInstanceId = asProviderInstanceId("claude_credential_boundary");
      const settingsForKey = (value: string): Partial<ServerSettings> => ({
        providerInstances: {
          claude_credential_boundary: {
            driver: "claudeAgent",
            enabled: true,
            environment: [{ name: "ANTHROPIC_API_KEY", value, sensitive: true }],
            config: { homePath: "/tmp/claude-credential-boundary" },
          },
        },
      });

      yield* serverSettings.updateSettings(settingsForKey("credential-v1"));
      const initial = yield* provider.startSession(threadId, {
        provider: "claudeAgent",
        providerInstanceId,
        threadId,
        runtimeMode: "full-access",
      });
      assert.ok(initial.resumeCursor);
      assert.equal(typeof provider.stopRuntimeSession, "function");
      if (!provider.stopRuntimeSession) return;
      yield* provider.stopRuntimeSession({ threadId });
      routing.claude.startSession.mockClear();

      yield* serverSettings.updateSettings(settingsForKey("credential-v2"));
      const result = yield* Effect.result(
        provider.startSession(threadId, {
          provider: "claudeAgent",
          providerInstanceId,
          threadId,
          runtimeMode: "full-access",
        }),
      );

      assert.equal(result._tag, "Failure");
      assert.equal(routing.claude.startSession.mock.calls.length, 0);
    }),
  );

  it.effect("rejects stopped Claude continuation on another instance at the same home", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService;
      const serverSettings = yield* ServerSettingsService;
      const threadId = asThreadId("thread-claude-stopped-instance-boundary");
      yield* serverSettings.updateSettings({
        providerInstances: {
          claude_personal: {
            driver: "claudeAgent",
            enabled: true,
            config: { homePath: "/tmp/claude-shared-home" },
          },
          claude_work: {
            driver: "claudeAgent",
            enabled: true,
            config: { homePath: "/tmp/claude-shared-home" },
          },
        },
      });
      const initial = yield* provider.startSession(threadId, {
        provider: "claudeAgent",
        providerInstanceId: asProviderInstanceId("claude_personal"),
        threadId,
        runtimeMode: "full-access",
      });
      assert.ok(initial.resumeCursor);
      assert.equal(typeof provider.stopRuntimeSession, "function");
      if (!provider.stopRuntimeSession) return;
      yield* provider.stopRuntimeSession({ threadId });
      routing.claude.startSession.mockClear();

      const result = yield* Effect.result(
        provider.startSession(threadId, {
          provider: "claudeAgent",
          providerInstanceId: asProviderInstanceId("claude_work"),
          threadId,
          runtimeMode: "full-access",
        }),
      );

      assert.equal(result._tag, "Failure");
      assert.equal(routing.claude.startSession.mock.calls.length, 0);
    }),
  );

  it.effect("rejects stale Claude recovery after credentials change at the same home", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService;
      const serverSettings = yield* ServerSettingsService;
      const threadId = asThreadId("thread-claude-recovery-credential-boundary");
      const providerInstanceId = asProviderInstanceId("claude_recovery_boundary");
      const settingsForKey = (value: string): Partial<ServerSettings> => ({
        providerInstances: {
          claude_recovery_boundary: {
            driver: "claudeAgent",
            enabled: true,
            environment: [{ name: "ANTHROPIC_API_KEY", value, sensitive: true }],
            config: { homePath: "/tmp/claude-recovery-boundary" },
          },
        },
      });

      yield* serverSettings.updateSettings(settingsForKey("credential-v1"));
      const initial = yield* provider.startSession(threadId, {
        provider: "claudeAgent",
        providerInstanceId,
        threadId,
        runtimeMode: "full-access",
      });
      assert.ok(initial.resumeCursor);
      yield* routing.claude.stopAll();
      routing.claude.startSession.mockClear();
      routing.claude.sendTurn.mockClear();

      yield* serverSettings.updateSettings(settingsForKey("credential-v2"));
      const result = yield* Effect.result(
        provider.sendTurn({
          threadId,
          input: "do not recover with changed credentials",
          attachments: [],
        }),
      );

      assert.equal(result._tag, "Failure");
      assert.equal(routing.claude.startSession.mock.calls.length, 0);
      assert.equal(routing.claude.sendTurn.mock.calls.length, 0);
    }),
  );

  it.effect("matches persisted launch options including server-only credentials", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService;
      const serverSettings = yield* ServerSettingsService;
      const threadId = asThreadId("thread-launch-options-match");
      const providerInstanceId = asProviderInstanceId("claude_match");

      yield* serverSettings.updateSettings({
        providerInstances: {
          claude_match: {
            driver: "claudeAgent",
            enabled: true,
            environment: [{ name: "ANTHROPIC_API_KEY", value: "env-v1", sensitive: true }],
            config: {
              homePath: "/tmp/claude-match",
            },
          },
        },
      });
      yield* provider.startSession(threadId, {
        provider: "claudeAgent",
        providerInstanceId,
        threadId,
        runtimeMode: "full-access",
      });
      assert.equal(typeof provider.sessionBindingMatchesLaunchOptions, "function");
      if (!provider.sessionBindingMatchesLaunchOptions) {
        assert.fail("sessionBindingMatchesLaunchOptions unavailable");
      }

      assert.equal(
        yield* provider.sessionBindingMatchesLaunchOptions({
          threadId,
          provider: "claudeAgent",
          providerInstanceId,
        }),
        true,
      );

      yield* serverSettings.updateSettings({
        providerInstances: {
          claude_match: {
            driver: "claudeAgent",
            enabled: true,
            environment: [{ name: "ANTHROPIC_API_KEY", value: "env-v2", sensitive: true }],
            config: {
              homePath: "/tmp/claude-match",
            },
          },
        },
      });

      assert.equal(
        yield* provider.sessionBindingMatchesLaunchOptions({
          threadId,
          provider: "claudeAgent",
          providerInstanceId,
        }),
        false,
      );
    }),
  );

  it.effect("rejects send turns whose model selection targets another provider", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService;
      routing.codex.sendTurn.mockClear();
      const session = yield* provider.startSession(asThreadId("thread-send-mismatch"), {
        provider: "codex",
        threadId: asThreadId("thread-send-mismatch"),
        runtimeMode: "full-access",
      });

      const result = yield* Effect.result(
        provider.sendTurn({
          threadId: session.threadId,
          input: "wrong route",
          attachments: [],
          modelSelection: {
            instanceId: "claudeAgent",
            model: "claude-opus-4-6",
          },
        }),
      );

      assertFailure(
        result,
        new ProviderValidationError({
          operation: "ProviderService.sendTurn",
          issue:
            "Model selection instance 'claudeAgent' does not match routed provider instance 'codex'.",
        }),
      );
      assert.equal(routing.codex.sendTurn.mock.calls.length, 0);
    }),
  );

  it.effect(
    "canonicalizes stale provider metadata when the selected instance matches the route",
    () =>
      Effect.gen(function* () {
        const provider = yield* ProviderService;
        const serverSettings = yield* ServerSettingsService;
        routing.claude.sendTurn.mockClear();

        yield* serverSettings.updateSettings({
          providerInstances: {
            claude_work: {
              driver: "claudeAgent",
              enabled: true,
              config: { homePath: "/tmp/claude-work" },
            },
          },
        });
        const session = yield* provider.startSession(asThreadId("thread-stale-provider-instance"), {
          provider: "claudeAgent",
          providerInstanceId: "claude_work",
          threadId: asThreadId("thread-stale-provider-instance"),
          runtimeMode: "full-access",
        });

        yield* provider.sendTurn({
          threadId: session.threadId,
          input: "stale provider label, exact instance",
          attachments: [],
          modelSelection: {
            instanceId: "claude_work",
            model: "custom-claude-model",
            options: [{ id: "reasoningEffort", value: "high" }],
          },
        });

        assert.equal(routing.claude.sendTurn.mock.calls.length, 1);
        assert.deepEqual(routing.claude.sendTurn.mock.calls[0]?.[0].modelSelection, {
          instanceId: "claude_work",
          model: "custom-claude-model",
          options: [{ id: "reasoningEffort", value: "high" }],
        });
      }),
  );

  it.effect(
    "routes early approval and user-input responses to live sessions before persistence",
    () =>
      Effect.gen(function* () {
        const provider = yield* ProviderService;
        const directory = yield* ProviderSessionDirectory;
        const threadId = asThreadId("thread-live-startup-prompt");

        routing.codex.respondToRequest.mockClear();
        routing.codex.respondToUserInput.mockClear();
        yield* routing.codex.adapter.startSession({
          provider: "codex",
          threadId,
          runtimeMode: "approval-required",
        });

        const bindingBeforeResponse = yield* directory.getBinding(threadId);
        assert.equal(Option.isNone(bindingBeforeResponse), true);

        yield* provider.respondToRequest({
          threadId,
          requestId: asRequestId("req-live-approval"),
          decision: "accept",
        });
        yield* provider.respondToUserInput({
          threadId,
          requestId: asRequestId("req-live-user-input"),
          answers: {
            answer: "continue",
          },
        });

        assert.deepEqual(routing.codex.respondToRequest.mock.calls, [
          [threadId, asRequestId("req-live-approval"), "accept"],
        ]);
        assert.deepEqual(routing.codex.respondToUserInput.mock.calls, [
          [
            threadId,
            asRequestId("req-live-user-input"),
            {
              answer: "continue",
            },
          ],
        ]);
      }),
  );

  it.effect("preserves provider instance id when adopting binding-less live sessions", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService;
      const serverSettings = yield* ServerSettingsService;
      const directory = yield* ProviderSessionDirectory;
      const threadId = asThreadId("thread-live-instance");

      yield* serverSettings.updateSettings({
        providerInstances: {
          codex_work: {
            driver: "codex",
            displayName: "Codex Work",
          },
        },
      });

      yield* routing.codex.adapter.startSession({
        provider: "codex",
        providerInstanceId: "codex_work",
        threadId,
        runtimeMode: "approval-required",
      });

      const bindingBeforeTurn = yield* directory.getBinding(threadId);
      assert.equal(Option.isNone(bindingBeforeTurn), true);

      yield* provider.sendTurn({
        threadId,
        input: "hello from work account",
        attachments: [],
      });

      const bindingAfterTurn = yield* directory.getBinding(threadId);
      assert.equal(Option.isSome(bindingAfterTurn), true);
      if (Option.isSome(bindingAfterTurn)) {
        assert.equal(bindingAfterTurn.value.provider, "codex");
        assert.equal(bindingAfterTurn.value.providerInstanceId, "codex_work");
      }
    }),
  );

  it.effect("does not adopt a live same-driver session from a different provider instance", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService;
      const serverSettings = yield* ServerSettingsService;
      const threadId = asThreadId("thread-cross-instance-recovery");
      const fixture = makeSharedCodexContinuationFixture(["personal", "work"]);

      yield* serverSettings.updateSettings({
        providerInstances: {
          codex_personal: {
            ...providerInstanceForSharedCodexFixture(fixture, "personal"),
            displayName: "Codex Personal",
          },
          codex_work: {
            ...providerInstanceForSharedCodexFixture(fixture, "work"),
            displayName: "Codex Work",
          },
        },
      });

      const initial = yield* provider.startSession(threadId, {
        provider: "codex",
        providerInstanceId: "codex_work",
        threadId,
        cwd: "/tmp/project-work",
        runtimeMode: "full-access",
      });
      yield* routing.codex.stopSession(threadId);
      yield* routing.codex.adapter.startSession({
        provider: "codex",
        providerInstanceId: "codex_personal",
        threadId,
        runtimeMode: "full-access",
      });
      routing.codex.stopSession.mockClear();
      routing.codex.startSession.mockClear();
      routing.codex.sendTurn.mockClear();

      yield* provider.sendTurn({
        threadId,
        input: "continue on the work account",
        attachments: [],
      });

      assert.equal(routing.codex.stopSession.mock.calls.length, 1);
      assert.equal(routing.codex.startSession.mock.calls.length, 1);
      const resumedStartInput = routing.codex.startSession.mock.calls[0]?.[0];
      assert.equal(typeof resumedStartInput === "object" && resumedStartInput !== null, true);
      if (resumedStartInput && typeof resumedStartInput === "object") {
        assert.equal(resumedStartInput.providerInstanceId, "codex_work");
        assert.deepEqual(resumedStartInput.resumeCursor, initial.resumeCursor);
        assert.equal(
          resumedStartInput.expectedCodexContinuationGeneration,
          requireSharedCodexFixtureGeneration(fixture, "work"),
        );
      }
      assert.equal(routing.codex.sendTurn.mock.calls.length, 1);
      fs.rmSync(fixture.root, { recursive: true, force: true });
    }),
  );

  it.effect("keeps an unstamped live same-driver session when the binding owns the instance", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService;
      const serverSettings = yield* ServerSettingsService;
      const threadId = asThreadId("thread-unstamped-live-instance");

      yield* serverSettings.updateSettings({
        providerInstances: {
          codex_work: {
            driver: "codex",
            displayName: "Codex Work",
          },
        },
      });

      yield* provider.startSession(threadId, {
        provider: "codex",
        providerInstanceId: "codex_work",
        threadId,
        cwd: "/tmp/project-work",
        runtimeMode: "full-access",
      });
      routing.codex.updateSession(threadId, (existing) => {
        const { providerInstanceId: _providerInstanceId, ...sessionWithoutInstance } = existing;
        return sessionWithoutInstance;
      });
      routing.codex.stopSession.mockClear();
      routing.codex.startSession.mockClear();
      routing.codex.sendTurn.mockClear();

      yield* provider.sendTurn({
        threadId,
        input: "continue on the unstamped work account",
        attachments: [],
      });

      assert.equal(routing.codex.stopSession.mock.calls.length, 0);
      assert.equal(routing.codex.startSession.mock.calls.length, 0);
      assert.equal(routing.codex.sendTurn.mock.calls.length, 1);
    }),
  );

  it.effect("recovers stale persisted sessions for rollback by resuming thread identity", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService;
      const serverSettings = yield* ServerSettingsService;
      const threadId = asThreadId("thread-stale-codex-rollback");
      const fixture = makeSharedCodexContinuationFixture(["default"]);
      const providerInstanceId = asProviderInstanceId("codex_stale_rollback");
      yield* serverSettings.updateSettings({
        providerInstances: {
          codex_stale_rollback: providerInstanceForSharedCodexFixture(fixture, "default"),
        },
      });

      const initial = yield* provider.startSession(threadId, {
        provider: "codex",
        providerInstanceId,
        threadId,
        cwd: "/tmp/project",
        runtimeMode: "full-access",
      });
      yield* routing.codex.stopSession(initial.threadId);
      routing.codex.startSession.mockClear();
      routing.codex.rollbackThread.mockClear();

      yield* provider.rollbackConversation({
        threadId: initial.threadId,
        numTurns: 1,
      });

      assert.equal(routing.codex.startSession.mock.calls.length, 1);
      const resumedStartInput = routing.codex.startSession.mock.calls[0]?.[0];
      assert.equal(typeof resumedStartInput === "object" && resumedStartInput !== null, true);
      if (resumedStartInput && typeof resumedStartInput === "object") {
        const startPayload = resumedStartInput as {
          provider?: string;
          cwd?: string;
          expectedCodexContinuationGeneration?: string;
          resumeCursor?: unknown;
          threadId?: string;
        };
        assert.equal(startPayload.provider, "codex");
        assert.equal(startPayload.cwd, "/tmp/project");
        assert.deepEqual(startPayload.resumeCursor, initial.resumeCursor);
        assert.equal(startPayload.threadId, initial.threadId);
        assert.equal(
          startPayload.expectedCodexContinuationGeneration,
          requireSharedCodexFixtureGeneration(fixture, "default"),
        );
      }
      assert.equal(routing.codex.rollbackThread.mock.calls.length, 1);
      const rollbackCall = routing.codex.rollbackThread.mock.calls[0];
      assert.equal(rollbackCall?.[1], 1);
      fs.rmSync(fixture.root, { recursive: true, force: true });
    }),
  );

  it.effect("routes explicit claudeAgent provider session starts to the claude adapter", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService;
      routing.claude.startSession.mockClear();

      const session = yield* provider.startSession(asThreadId("thread-claude"), {
        provider: "claudeAgent",
        threadId: asThreadId("thread-claude"),
        cwd: "/tmp/project-claude",
        runtimeMode: "full-access",
      });

      assert.equal(session.provider, "claudeAgent");
      assert.equal(routing.claude.startSession.mock.calls.length, 1);
      const startInput = routing.claude.startSession.mock.calls[0]?.[0];
      assert.equal(typeof startInput === "object" && startInput !== null, true);
      if (startInput && typeof startInput === "object") {
        const startPayload = startInput as { provider?: string; cwd?: string };
        assert.equal(startPayload.provider, "claudeAgent");
        assert.equal(startPayload.cwd, "/tmp/project-claude");
      }
    }),
  );

  it.effect("recovers stale sessions for sendTurn using persisted cwd", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService;
      const serverSettings = yield* ServerSettingsService;
      const threadId = asThreadId("thread-stale-codex-send-turn");
      const fixture = makeSharedCodexContinuationFixture(["default"]);
      const providerInstanceId = asProviderInstanceId("codex_stale_send_turn");
      yield* serverSettings.updateSettings({
        providerInstances: {
          codex_stale_send_turn: providerInstanceForSharedCodexFixture(fixture, "default"),
        },
      });

      const initial = yield* provider.startSession(threadId, {
        provider: "codex",
        providerInstanceId,
        threadId,
        cwd: "/tmp/project-send-turn",
        runtimeMode: "full-access",
      });

      yield* routing.codex.stopAll();
      routing.codex.startSession.mockClear();
      routing.codex.sendTurn.mockClear();

      yield* provider.sendTurn({
        threadId: initial.threadId,
        input: "resume",
        attachments: [],
      });

      assert.equal(routing.codex.startSession.mock.calls.length, 1);
      const resumedStartInput = routing.codex.startSession.mock.calls[0]?.[0];
      assert.equal(typeof resumedStartInput === "object" && resumedStartInput !== null, true);
      if (resumedStartInput && typeof resumedStartInput === "object") {
        const startPayload = resumedStartInput as {
          provider?: string;
          cwd?: string;
          expectedCodexContinuationGeneration?: string;
          resumeCursor?: unknown;
          threadId?: string;
        };
        assert.equal(startPayload.provider, "codex");
        assert.equal(startPayload.cwd, "/tmp/project-send-turn");
        assert.deepEqual(startPayload.resumeCursor, initial.resumeCursor);
        assert.equal(startPayload.threadId, initial.threadId);
        assert.equal(
          startPayload.expectedCodexContinuationGeneration,
          requireSharedCodexFixtureGeneration(fixture, "default"),
        );
      }
      assert.equal(routing.codex.sendTurn.mock.calls.length, 1);
      fs.rmSync(fixture.root, { recursive: true, force: true });
    }),
  );

  it.effect("recovers stale claudeAgent sessions for sendTurn using persisted cwd", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService;

      const initial = yield* provider.startSession(asThreadId("thread-claude-send-turn"), {
        threadId: asThreadId("thread-claude-send-turn"),
        cwd: "/tmp/project-claude-send-turn",
        modelSelection: {
          instanceId: "claudeAgent",
          model: "claude-opus-4-6",
          options: [{ id: "effort", value: "max" }],
        },
        runtimeMode: "full-access",
      });

      yield* routing.claude.stopAll();
      routing.claude.startSession.mockClear();
      routing.claude.sendTurn.mockClear();

      yield* provider.sendTurn({
        threadId: initial.threadId,
        input: "resume with claude",
        attachments: [],
      });

      assert.equal(routing.claude.startSession.mock.calls.length, 1);
      const resumedStartInput = routing.claude.startSession.mock.calls[0]?.[0];
      assert.equal(typeof resumedStartInput === "object" && resumedStartInput !== null, true);
      if (resumedStartInput && typeof resumedStartInput === "object") {
        const startPayload = resumedStartInput as {
          provider?: string;
          cwd?: string;
          modelSelection?: unknown;
          resumeCursor?: unknown;
          threadId?: string;
        };
        assert.equal(startPayload.provider, "claudeAgent");
        assert.equal(startPayload.cwd, "/tmp/project-claude-send-turn");
        assert.deepEqual(startPayload.modelSelection, {
          instanceId: "claudeAgent",
          model: "claude-opus-4-6",
          options: [{ id: "effort", value: "max" }],
        });
        assert.deepEqual(startPayload.resumeCursor, initial.resumeCursor);
        assert.equal(startPayload.threadId, initial.threadId);
      }
      assert.equal(routing.claude.sendTurn.mock.calls.length, 1);
    }),
  );

  it.effect("lists no sessions after adapter runtime clears", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService;
      const firstThreadId = asThreadId("thread-list-cleared-1");
      const secondThreadId = asThreadId("thread-list-cleared-2");

      yield* provider.startSession(firstThreadId, {
        provider: "codex",
        threadId: firstThreadId,
        runtimeMode: "full-access",
      });
      yield* provider.startSession(secondThreadId, {
        provider: "codex",
        threadId: secondThreadId,
        runtimeMode: "full-access",
      });

      yield* routing.codex.stopAll();
      yield* routing.claude.stopAll();

      const remaining = yield* provider.listSessions();
      assert.equal(remaining.length, 0);
    }),
  );

  it.effect("persists runtime status transitions in provider_session_runtime", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService;
      const runtimeRepository = yield* ProviderSessionRuntimeRepository;
      const threadId = asThreadId("thread-runtime-status-transitions");

      const session = yield* provider.startSession(threadId, {
        provider: "codex",
        threadId,
        runtimeMode: "full-access",
      });
      yield* provider.sendTurn({
        threadId: session.threadId,
        input: "hello",
        attachments: [],
      });

      const runningRuntime = yield* runtimeRepository.getByThreadId({
        threadId: session.threadId,
      });
      assert.equal(Option.isSome(runningRuntime), true);
      if (Option.isSome(runningRuntime)) {
        assert.equal(runningRuntime.value.status, "running");
        assert.deepEqual(runningRuntime.value.resumeCursor, session.resumeCursor);
        const payload = runningRuntime.value.runtimePayload;
        assert.equal(payload !== null && typeof payload === "object", true);
        if (payload !== null && typeof payload === "object" && !Array.isArray(payload)) {
          const runtimePayload = payload as {
            cwd: string;
            model: string | null;
            activeTurnId: string | null;
            lastError: string | null;
            lastRuntimeEvent: string | null;
          };
          assert.equal(runtimePayload.cwd, process.cwd());
          assert.equal(runtimePayload.model, null);
          assert.equal(runtimePayload.activeTurnId, `turn-${String(session.threadId)}`);
          assert.equal(runtimePayload.lastError, null);
          assert.equal(runtimePayload.lastRuntimeEvent, "provider.sendTurn");
        }
      }
    }),
  );

  it.effect("clears persisted active turn metadata when a runtime turn completes", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService;
      const runtimeRepository = yield* ProviderSessionRuntimeRepository;

      const session = yield* provider.startSession(asThreadId("thread-runtime-complete"), {
        provider: "codex",
        threadId: asThreadId("thread-runtime-complete"),
        runtimeMode: "full-access",
      });
      const turn = yield* provider.sendTurn({
        threadId: session.threadId,
        input: "hello",
        attachments: [],
        modelSelection: {
          instanceId: "codex",
          model: "gpt-5-codex",
        },
      });
      yield* sleep(50);

      routing.codex.emit({
        type: "turn.completed",
        eventId: asEventId("runtime-complete-event"),
        provider: "codex",
        createdAt: "2026-02-27T00:04:00.000Z",
        threadId: session.threadId,
        turnId: turn.turnId,
        payload: { state: "completed" },
      });
      yield* sleep(50);

      const runtime = yield* runtimeRepository.getByThreadId({
        threadId: session.threadId,
      });
      assert.equal(Option.isSome(runtime), true);
      if (Option.isSome(runtime)) {
        assert.equal(runtime.value.status, "stopped");
        const payload = runtime.value.runtimePayload;
        assert.equal(payload !== null && typeof payload === "object", true);
        if (payload !== null && typeof payload === "object" && !Array.isArray(payload)) {
          const runtimePayload = payload as {
            activeTurnId: string | null;
            lastRuntimeEvent: string | null;
            modelSelection?: unknown;
          };
          assert.equal(runtimePayload.activeTurnId, null);
          assert.equal(runtimePayload.lastRuntimeEvent, "turn.completed");
          assert.deepEqual(runtimePayload.modelSelection, {
            instanceId: "codex",
            model: "gpt-5-codex",
          });
        }
      }
    }),
  );

  it.effect("keeps a newer binding active when an overlapping older turn completes late", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService;
      const directory = yield* ProviderSessionDirectory;
      const threadId = asThreadId("thread-overlapping-stale-terminal");
      const olderTurnId = asTurnId("turn-overlapping-older");
      const newerTurnId = asTurnId("turn-overlapping-newer");
      const olderResumeCursor = { cursor: "older-resume" };
      const newerResumeCursor = { cursor: "newer-resume" };
      const olderModelSelection = { instanceId: "codex" as const, model: "gpt-5.1-codex-mini" };
      const newerModelSelection = {
        instanceId: "codex" as const,
        model: "gpt-5-codex-mini",
      };
      let olderDispatchStarted = false;
      let releaseOlderDispatch: ((result: ProviderTurnStartResult) => void) | undefined;

      yield* provider.startSession(threadId, {
        provider: "codex",
        threadId,
        runtimeMode: "full-access",
      });
      routing.codex.sendTurn
        .mockImplementationOnce(() =>
          Effect.promise(
            () =>
              new Promise<ProviderTurnStartResult>((resolve) => {
                olderDispatchStarted = true;
                releaseOlderDispatch = resolve;
              }),
          ),
        )
        .mockImplementationOnce((input) =>
          Effect.succeed({
            threadId: input.threadId,
            turnId: newerTurnId,
            resumeCursor: newerResumeCursor,
          }),
        );

      const olderSendFiber = yield* provider
        .sendTurn({
          threadId,
          input: "older",
          attachments: [],
          modelSelection: olderModelSelection,
        })
        .pipe(Effect.forkChild);
      yield* waitUntil(() => olderDispatchStarted, 500, 20, "older turn dispatch");
      yield* provider.sendTurn({
        threadId,
        input: "newer",
        attachments: [],
        modelSelection: newerModelSelection,
      });

      yield* routing.codex.waitForRuntimeSubscribers();
      routing.codex.emit({
        type: "turn.completed",
        eventId: asEventId("runtime-overlapping-older-completed"),
        provider: "codex",
        createdAt: "2026-02-27T00:04:00.000Z",
        threadId,
        turnId: olderTurnId,
        payload: { state: "completed" },
      });
      yield* sleep(50);

      const release = releaseOlderDispatch;
      if (!release) {
        assert.fail("Expected delayed older dispatch release callback");
      }
      release({ threadId, turnId: olderTurnId, resumeCursor: olderResumeCursor });
      yield* Fiber.join(olderSendFiber);

      const binding = Option.getOrUndefined(yield* directory.getBinding(threadId));
      const runtimePayload = asRuntimePayloadRecord(binding?.runtimePayload);
      assert.equal(binding?.status, "running");
      assert.deepEqual(binding?.resumeCursor, newerResumeCursor);
      assert.equal(runtimePayload.activeTurnId, newerTurnId);
      assert.equal(runtimePayload.lastRuntimeEvent, "provider.sendTurn");
      assert.deepEqual(runtimePayload.modelSelection, newerModelSelection);
    }),
  );

  it.effect("keeps the newer invocation active when an older dispatch returns last", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService;
      const directory = yield* ProviderSessionDirectory;
      const threadId = asThreadId("thread-overlapping-return-order");
      const olderTurnId = asTurnId("turn-return-order-older");
      const newerTurnId = asTurnId("turn-return-order-newer");
      let releaseOlder: ((result: ProviderTurnStartResult) => void) | undefined;

      yield* provider.startSession(threadId, {
        provider: "codex",
        threadId,
        runtimeMode: "full-access",
      });
      routing.codex.sendTurn
        .mockImplementationOnce(() =>
          Effect.promise(
            () =>
              new Promise<ProviderTurnStartResult>((resolve) => {
                releaseOlder = resolve;
              }),
          ),
        )
        .mockImplementationOnce((input) =>
          Effect.succeed({
            threadId: input.threadId,
            turnId: newerTurnId,
            resumeCursor: { cursor: "newer" },
          }),
        );

      const olderFiber = yield* provider
        .sendTurn({ threadId, input: "older", attachments: [] })
        .pipe(Effect.forkChild);
      yield* waitUntil(() => releaseOlder !== undefined, 500, 20, "older dispatch start");
      yield* provider.sendTurn({ threadId, input: "newer", attachments: [] });
      releaseOlder?.({ threadId, turnId: olderTurnId, resumeCursor: { cursor: "older" } });
      yield* Fiber.join(olderFiber);

      const binding = Option.getOrUndefined(yield* directory.getBinding(threadId));
      const payload = binding?.runtimePayload as Record<string, unknown>;
      assert.equal(payload.activeTurnId, newerTurnId);
      assert.deepEqual(binding?.resumeCursor, { cursor: "newer" });
    }),
  );

  it.effect("promotes an older successful dispatch when the newer invocation fails", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService;
      const directory = yield* ProviderSessionDirectory;
      const threadId = asThreadId("thread-promote-older-success");
      const olderTurnId = asTurnId("turn-promoted-older");
      const olderCursor = { cursor: "promoted-older" };
      const olderModelSelection = { instanceId: "codex" as const, model: "gpt-5-codex" };
      const newerFailure = new ProviderAdapterSessionNotFoundError({
        provider: "codex",
        threadId,
      });
      let releaseOlder: ((result: ProviderTurnStartResult) => void) | undefined;
      let failNewer: (() => void) | undefined;

      yield* provider.startSession(threadId, {
        provider: "codex",
        threadId,
        runtimeMode: "full-access",
      });
      routing.codex.sendTurn
        .mockImplementationOnce(() =>
          Effect.promise(
            () =>
              new Promise<ProviderTurnStartResult>((resolve) => {
                releaseOlder = resolve;
              }),
          ),
        )
        .mockImplementationOnce(() =>
          Effect.promise(
            () =>
              new Promise<void>((resolve) => {
                failNewer = resolve;
              }),
          ).pipe(Effect.andThen(Effect.fail(newerFailure))),
        );

      const olderFiber = yield* provider
        .sendTurn({
          threadId,
          input: "older",
          attachments: [],
          modelSelection: olderModelSelection,
        })
        .pipe(Effect.forkChild);
      yield* waitUntil(() => releaseOlder !== undefined, 500, 20, "older dispatch start");
      const newerFiber = yield* provider
        .sendTurn({ threadId, input: "newer", attachments: [] })
        .pipe(Effect.forkChild);
      yield* waitUntil(() => failNewer !== undefined, 500, 20, "newer dispatch start");

      releaseOlder?.({ threadId, turnId: olderTurnId, resumeCursor: olderCursor });
      yield* Fiber.join(olderFiber);
      const beforeNewerFailure = Option.getOrUndefined(yield* directory.getBinding(threadId));
      const beforeFailurePayload = beforeNewerFailure?.runtimePayload as
        | Record<string, unknown>
        | undefined;
      assert.notEqual(beforeFailurePayload?.activeTurnId, olderTurnId);

      failNewer?.();
      const failedResult = yield* Effect.result(Fiber.join(newerFiber));
      assertFailure(failedResult, newerFailure);

      const binding = Option.getOrUndefined(yield* directory.getBinding(threadId));
      const payload = binding?.runtimePayload as Record<string, unknown>;
      assert.equal(binding?.status, "running");
      assert.equal(payload.activeTurnId, olderTurnId);
      assert.deepEqual(binding?.resumeCursor, olderCursor);
      assert.deepEqual(payload.modelSelection, olderModelSelection);
    }),
  );

  it.effect("rolls back turn bookkeeping when started-turn persistence fails", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService;
      const directory = yield* ProviderSessionDirectory;
      const threadId = asThreadId("thread-started-persistence-failure");
      const failedTurnId = asTurnId("turn-persistence-failed");
      const nextTurnId = asTurnId("turn-after-persistence-failure");
      const persistenceFailure = new ProviderSessionDirectoryPersistenceError({
        operation: "test",
        detail: "injected started-turn persistence failure",
      });

      yield* provider.startSession(threadId, {
        provider: "codex",
        threadId,
        runtimeMode: "full-access",
      });
      routing.codex.sendTurn
        .mockImplementationOnce((input) =>
          Effect.succeed({ threadId: input.threadId, turnId: failedTurnId }),
        )
        .mockImplementationOnce((input) =>
          Effect.succeed({ threadId: input.threadId, turnId: nextTurnId }),
        );
      const upsertSpy = vi
        .spyOn(directory, "upsert")
        .mockImplementationOnce(() => Effect.fail(persistenceFailure));

      const failedResult = yield* Effect.result(
        provider.sendTurn({ threadId, input: "fails to persist", attachments: [] }),
      );
      assertFailure(failedResult, persistenceFailure);
      upsertSpy.mockRestore();

      yield* provider.sendTurn({ threadId, input: "next turn", attachments: [] });
      yield* routing.codex.waitForRuntimeSubscribers();
      routing.codex.emit({
        type: "turn.completed",
        eventId: asEventId("runtime-unscoped-after-persistence-failure"),
        provider: "codex",
        createdAt: "2026-02-27T00:04:00.000Z",
        threadId,
        payload: { state: "completed" },
      });
      yield* sleep(50);

      const binding = Option.getOrUndefined(yield* directory.getBinding(threadId));
      const payload = binding?.runtimePayload as Record<string, unknown>;
      assert.equal(binding?.status, "stopped");
      assert.equal(payload.activeTurnId, null);
      assert.equal(payload.lastRuntimeEvent, "turn.completed");
    }),
  );

  it.effect("persists steer turn lifecycle, cursor, and model metadata", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService;
      const directory = yield* ProviderSessionDirectory;
      const threadId = asThreadId("thread-steer-persistence");
      const turnId = asTurnId("turn-steer-persistence");
      const resumeCursor = { cursor: "steer-resume" };
      const modelSelection = {
        instanceId: "codex" as const,
        model: "gpt-5-codex-mini",
      };

      yield* provider.startSession(threadId, {
        provider: "codex",
        threadId,
        runtimeMode: "full-access",
      });
      routing.codex.steerTurn.mockImplementationOnce((input) =>
        Effect.succeed({ threadId: input.threadId, turnId, resumeCursor }),
      );

      yield* provider.steerTurn({
        threadId,
        input: "steer toward this",
        attachments: [],
        modelSelection,
      });

      const binding = Option.getOrUndefined(yield* directory.getBinding(threadId));
      const runtimePayload = asRuntimePayloadRecord(binding?.runtimePayload);
      assert.equal(binding?.status, "running");
      assert.deepEqual(binding?.resumeCursor, resumeCursor);
      assert.equal(runtimePayload.activeTurnId, turnId);
      assert.equal(runtimePayload.lastRuntimeEvent, "provider.steerTurn");
      assert.deepEqual(runtimePayload.modelSelection, modelSelection);
    }),
  );

  it.effect("keeps a newer review binding when an older steer returns late", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService;
      const directory = yield* ProviderSessionDirectory;
      const threadId = asThreadId("thread-review-newer-generation");
      const staleSteerTurnId = asTurnId("turn-stale-steer");
      const reviewTurnId = asTurnId("turn-newer-review");
      const staleSteerCursor = { cursor: "stale-steer-resume" };
      const reviewCursor = { cursor: "newer-review-resume" };
      const initialModelSelection = { instanceId: "codex" as const, model: "gpt-5-codex" };
      const staleSteerModelSelection = {
        instanceId: "codex" as const,
        model: "gpt-5-codex-mini",
      };
      let steerStarted = false;
      let releaseSteer: ((result: ProviderTurnStartResult) => void) | undefined;

      yield* provider.startSession(threadId, {
        provider: "codex",
        threadId,
        runtimeMode: "full-access",
        modelSelection: initialModelSelection,
      });
      routing.codex.steerTurn.mockImplementationOnce(() =>
        Effect.promise(
          () =>
            new Promise<ProviderTurnStartResult>((resolve) => {
              steerStarted = true;
              releaseSteer = resolve;
            }),
        ),
      );
      routing.codex.startReview.mockImplementationOnce((input) =>
        Effect.succeed({
          threadId: input.threadId,
          turnId: reviewTurnId,
          resumeCursor: reviewCursor,
        }),
      );

      const steerFiber = yield* provider
        .steerTurn({
          threadId,
          input: "older steer",
          attachments: [],
          modelSelection: staleSteerModelSelection,
        })
        .pipe(Effect.forkChild);
      yield* waitUntil(() => steerStarted, 500, 20, "delayed steer dispatch");

      yield* provider.startReview({
        threadId,
        target: { type: "uncommittedChanges" },
      });

      const release = releaseSteer;
      if (!release) {
        assert.fail("Expected delayed steer release callback");
      }
      release({ threadId, turnId: staleSteerTurnId, resumeCursor: staleSteerCursor });
      yield* Fiber.join(steerFiber);

      const binding = Option.getOrUndefined(yield* directory.getBinding(threadId));
      const runtimePayload = asRuntimePayloadRecord(binding?.runtimePayload);
      assert.equal(binding?.status, "running");
      assert.deepEqual(binding?.resumeCursor, reviewCursor);
      assert.equal(runtimePayload.activeTurnId, reviewTurnId);
      assert.equal(runtimePayload.lastRuntimeEvent, "provider.startReview");
      assert.deepEqual(runtimePayload.modelSelection, initialModelSelection);
    }),
  );

  it.effect("refreshes persisted resume cursor immediately on model reroutes", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService;
      const runtimeRepository = yield* ProviderSessionRuntimeRepository;

      const session = yield* provider.startSession(asThreadId("thread-runtime-resume-refresh"), {
        provider: "claudeAgent",
        threadId: asThreadId("thread-runtime-resume-refresh"),
        runtimeMode: "full-access",
      });
      const updatedResumeCursor = {
        threadId: session.threadId,
        resume: "550e8400-e29b-41d4-a716-446655440000",
        resumeSessionAt: "assistant-message-refresh",
        turnCount: 2,
        rerouteOriginalApiModelId: "claude-fable-5",
        rerouteFallbackApiModelId: "claude-opus-4-8",
      };

      routing.claude.updateSession(session.threadId, (existing) => ({
        ...existing,
        resumeCursor: updatedResumeCursor,
      }));
      routing.claude.emit({
        type: "model.rerouted",
        eventId: asEventId("runtime-model-rerouted-refresh"),
        provider: "claudeAgent",
        createdAt: "2026-02-27T00:04:00.000Z",
        threadId: session.threadId,
        payload: {
          fromModel: "claude-fable-5",
          toModel: "claude-opus-4-8",
          reason: "Model safeguards rerouted this request.",
        },
      });
      yield* sleep(50);

      const runtime = yield* runtimeRepository.getByThreadId({
        threadId: session.threadId,
      });
      assert.equal(Option.isSome(runtime), true);
      if (Option.isSome(runtime)) {
        assert.deepEqual(runtime.value.resumeCursor, updatedResumeCursor);
      }
    }),
  );

  it.effect("persists task-list resume state before the active turn completes", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService;
      const runtimeRepository = yield* ProviderSessionRuntimeRepository;

      const session = yield* provider.startSession(asThreadId("thread-task-resume-refresh"), {
        provider: "claudeAgent",
        threadId: asThreadId("thread-task-resume-refresh"),
        runtimeMode: "full-access",
      });
      const turn = yield* provider.sendTurn({
        threadId: session.threadId,
        input: "continue the work",
        attachments: [],
      });
      const updatedResumeCursor = {
        threadId: session.threadId,
        resume: "550e8400-e29b-41d4-a716-446655440000",
        turnCount: 1,
        trackedTasks: [
          {
            id: "task-1",
            subject: "Patch UI",
            status: "in_progress",
            blockedBy: [],
          },
        ],
      };

      routing.claude.updateSession(session.threadId, (existing) => ({
        ...existing,
        resumeCursor: updatedResumeCursor,
      }));
      routing.claude.emit({
        type: "turn.tasks.updated",
        eventId: asEventId("runtime-task-resume-refresh"),
        provider: "claudeAgent",
        createdAt: "2026-02-27T00:04:30.000Z",
        threadId: session.threadId,
        turnId: turn.turnId,
        payload: {
          tasks: [{ task: "Patching UI", status: "inProgress" }],
        },
      });

      yield* waitUntilEffect(
        () =>
          runtimeRepository.getByThreadId({ threadId: session.threadId }).pipe(
            Effect.map(
              Option.exists((runtime) => {
                const cursor = runtime.resumeCursor;
                return cursor !== null && typeof cursor === "object" && "trackedTasks" in cursor;
              }),
            ),
          ),
        500,
        20,
        "task resume cursor persistence",
      );

      const runtime = yield* runtimeRepository.getByThreadId({
        threadId: session.threadId,
      });
      assert.equal(Option.isSome(runtime), true);
      if (Option.isSome(runtime)) {
        assert.deepEqual(runtime.value.resumeCursor, updatedResumeCursor);
        assert.equal(runtime.value.status, "running");
      }
    }),
  );

  it.effect("marks persisted runtime bindings errored on runtime errors", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService;
      const runtimeRepository = yield* ProviderSessionRuntimeRepository;

      const session = yield* provider.startSession(asThreadId("thread-runtime-error"), {
        provider: "codex",
        threadId: asThreadId("thread-runtime-error"),
        runtimeMode: "full-access",
      });
      const turn = yield* provider.sendTurn({
        threadId: session.threadId,
        input: "hello",
        attachments: [],
      });

      routing.codex.emit({
        type: "runtime.error",
        eventId: asEventId("runtime-error-event"),
        provider: "codex",
        createdAt: "2026-02-27T00:05:00.000Z",
        threadId: session.threadId,
        turnId: turn.turnId,
        payload: { message: "Provider crashed", class: "provider_error" },
      });
      yield* sleep(50);

      const runtime = yield* runtimeRepository.getByThreadId({
        threadId: session.threadId,
      });
      assert.equal(Option.isSome(runtime), true);
      if (Option.isSome(runtime)) {
        assert.equal(runtime.value.status, "error");
        const payload = runtime.value.runtimePayload;
        assert.equal(payload !== null && typeof payload === "object", true);
        if (payload !== null && typeof payload === "object" && !Array.isArray(payload)) {
          const runtimePayload = payload as {
            activeTurnId: string | null;
            lastError: string | null;
            lastRuntimeEvent: string | null;
          };
          assert.equal(runtimePayload.activeTurnId, null);
          assert.equal(runtimePayload.lastError, "Provider crashed");
          assert.equal(runtimePayload.lastRuntimeEvent, "runtime.error");
        }
      }
    }),
  );

  it.effect("marks terminal thread state changes stopped or errored", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService;
      const runtimeRepository = yield* ProviderSessionRuntimeRepository;

      const session = yield* provider.startSession(asThreadId("thread-runtime-state-error"), {
        provider: "codex",
        threadId: asThreadId("thread-runtime-state-error"),
        runtimeMode: "full-access",
      });

      routing.codex.emit({
        type: "thread.state.changed",
        eventId: asEventId("runtime-thread-state-error"),
        provider: "codex",
        createdAt: "2026-02-27T00:05:00.000Z",
        threadId: session.threadId,
        payload: { state: "error" },
      });
      yield* sleep(50);

      const runtime = yield* runtimeRepository.getByThreadId({
        threadId: session.threadId,
      });
      assert.equal(Option.isSome(runtime), true);
      if (Option.isSome(runtime)) {
        assert.equal(runtime.value.status, "error");
        const payload = runtime.value.runtimePayload;
        assert.equal(payload !== null && typeof payload === "object", true);
        if (payload !== null && typeof payload === "object" && !Array.isArray(payload)) {
          assert.equal((payload as Record<string, unknown>).activeTurnId, null);
          assert.equal(
            (payload as Record<string, unknown>).lastRuntimeEvent,
            "thread.state.changed",
          );
        }
      }
    }),
  );

  it.effect("preserves active turns across compacted thread state boundaries", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService;
      const runtimeRepository = yield* ProviderSessionRuntimeRepository;

      const session = yield* provider.startSession(asThreadId("thread-runtime-compact-boundary"), {
        provider: "codex",
        threadId: asThreadId("thread-runtime-compact-boundary"),
        runtimeMode: "full-access",
      });
      const turn = yield* provider.sendTurn({
        threadId: session.threadId,
        input: "hello",
        attachments: [],
      });

      routing.codex.emit({
        type: "thread.state.changed",
        eventId: asEventId("runtime-thread-compact-boundary"),
        provider: "codex",
        createdAt: "2026-02-27T00:05:00.000Z",
        threadId: session.threadId,
        payload: { state: "compacted" },
      });
      yield* sleep(50);

      const runtime = yield* runtimeRepository.getByThreadId({
        threadId: session.threadId,
      });
      assert.equal(Option.isSome(runtime), true);
      if (Option.isSome(runtime)) {
        assert.equal(runtime.value.status, "running");
        const payload = runtime.value.runtimePayload;
        assert.equal(payload !== null && typeof payload === "object", true);
        if (payload !== null && typeof payload === "object" && !Array.isArray(payload)) {
          assert.equal((payload as Record<string, unknown>).activeTurnId, turn.turnId);
          assert.equal(
            (payload as Record<string, unknown>).lastRuntimeEvent,
            "thread.state.changed",
          );
        }
      }
    }),
  );

  it.effect("reuses persisted resume cursor when startSession is called after a restart", () =>
    Effect.gen(function* () {
      const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "synara-provider-service-start-"));
      const dbPath = path.join(tempDir, "orchestration.sqlite");
      const persistenceLayer = makeSqlitePersistenceLive(dbPath);
      const runtimeRepositoryLayer = ProviderSessionRuntimeRepositoryLive.pipe(
        Layer.provide(persistenceLayer),
      );

      const firstClaude = makeFakeCodexAdapter("claudeAgent");
      const firstRegistry: typeof ProviderAdapterRegistry.Service = {
        getByProvider: (provider) =>
          provider === "claudeAgent"
            ? Effect.succeed(firstClaude.adapter)
            : Effect.fail(new ProviderUnsupportedError({ provider })),
        listProviders: () => Effect.succeed(["claudeAgent"]),
      };
      const firstDirectoryLayer = ProviderSessionDirectoryLive.pipe(
        Layer.provide(runtimeRepositoryLayer),
      );
      const firstProviderLayer = makeProviderServiceLive().pipe(
        Layer.provide(Layer.succeed(ProviderAdapterRegistry, firstRegistry)),
        Layer.provide(firstDirectoryLayer),
        Layer.provide(AnalyticsService.layerTest),
        Layer.provide(ServerSettingsService.layerTest()),
        Layer.provide(ProviderServiceTestSecretStoreLayer),
      );

      const initial = yield* Effect.gen(function* () {
        const provider = yield* ProviderService;
        return yield* provider.startSession(asThreadId("thread-claude-start"), {
          provider: "claudeAgent",
          threadId: asThreadId("thread-claude-start"),
          cwd: "/tmp/project-claude-start",
          runtimeMode: "full-access",
        });
      }).pipe(Effect.provide(firstProviderLayer));

      yield* Effect.gen(function* () {
        const provider = yield* ProviderService;
        yield* provider.listSessions();
      }).pipe(Effect.provide(firstProviderLayer));

      const secondClaude = makeFakeCodexAdapter("claudeAgent");
      const secondRegistry: typeof ProviderAdapterRegistry.Service = {
        getByProvider: (provider) =>
          provider === "claudeAgent"
            ? Effect.succeed(secondClaude.adapter)
            : Effect.fail(new ProviderUnsupportedError({ provider })),
        listProviders: () => Effect.succeed(["claudeAgent"]),
      };
      const secondDirectoryLayer = ProviderSessionDirectoryLive.pipe(
        Layer.provide(runtimeRepositoryLayer),
      );
      const secondProviderLayer = makeProviderServiceLive().pipe(
        Layer.provide(Layer.succeed(ProviderAdapterRegistry, secondRegistry)),
        Layer.provide(secondDirectoryLayer),
        Layer.provide(AnalyticsService.layerTest),
        Layer.provide(ServerSettingsService.layerTest()),
        Layer.provide(ProviderServiceTestSecretStoreLayer),
      );

      secondClaude.startSession.mockClear();

      yield* Effect.gen(function* () {
        const provider = yield* ProviderService;
        yield* provider.startSession(initial.threadId, {
          provider: "claudeAgent",
          threadId: initial.threadId,
          cwd: "/tmp/project-claude-start",
          runtimeMode: "full-access",
        });
      }).pipe(Effect.provide(secondProviderLayer));

      assert.equal(secondClaude.startSession.mock.calls.length, 1);
      const resumedStartInput = secondClaude.startSession.mock.calls[0]?.[0];
      assert.equal(typeof resumedStartInput === "object" && resumedStartInput !== null, true);
      if (resumedStartInput && typeof resumedStartInput === "object") {
        const startPayload = resumedStartInput as {
          provider?: string;
          cwd?: string;
          resumeCursor?: unknown;
          threadId?: string;
        };
        assert.equal(startPayload.provider, "claudeAgent");
        assert.equal(startPayload.cwd, "/tmp/project-claude-start");
        assert.deepEqual(startPayload.resumeCursor, initial.resumeCursor);
        assert.equal(startPayload.threadId, initial.threadId);
      }

      fs.rmSync(tempDir, { recursive: true, force: true });
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("clears stale resume cursor and provider options for fresh restart", () =>
    Effect.gen(function* () {
      const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "synara-provider-service-clear-"));
      const dbPath = path.join(tempDir, "orchestration.sqlite");
      const persistenceLayer = makeSqlitePersistenceLive(dbPath);
      const runtimeRepositoryLayer = ProviderSessionRuntimeRepositoryLive.pipe(
        Layer.provide(persistenceLayer),
      );
      const providerOptions = {
        codex: {
          homePath: "/tmp/custom-codex-home",
          binaryPath: "/usr/local/bin/codex",
        },
      };

      const firstCodex = makeFakeCodexAdapter("codex");
      const firstRegistry: typeof ProviderAdapterRegistry.Service = {
        getByProvider: (provider) =>
          provider === "codex"
            ? Effect.succeed(firstCodex.adapter)
            : Effect.fail(new ProviderUnsupportedError({ provider })),
        listProviders: () => Effect.succeed(["codex"]),
      };
      const firstDirectoryLayer = ProviderSessionDirectoryLive.pipe(
        Layer.provide(runtimeRepositoryLayer),
      );
      const firstProviderLayer = makeProviderServiceLive().pipe(
        Layer.provide(Layer.succeed(ProviderAdapterRegistry, firstRegistry)),
        Layer.provide(firstDirectoryLayer),
        Layer.provide(AnalyticsService.layerTest),
        Layer.provide(ServerSettingsService.layerTest()),
        Layer.provide(ProviderServiceTestSecretStoreLayer),
      );

      const initial = yield* Effect.gen(function* () {
        const provider = yield* ProviderService;
        const session = yield* provider.startSession(asThreadId("thread-clear-resume"), {
          provider: "codex",
          threadId: asThreadId("thread-clear-resume"),
          cwd: "/tmp/project-clear-resume",
          providerOptions,
          runtimeMode: "full-access",
        });
        assert.equal(typeof provider.clearSessionResumeCursor, "function");
        if (provider.clearSessionResumeCursor) {
          yield* provider.clearSessionResumeCursor({ threadId: session.threadId });
        }
        return session;
      }).pipe(Effect.provide(firstProviderLayer));

      const secondCodex = makeFakeCodexAdapter("codex");
      const secondRegistry: typeof ProviderAdapterRegistry.Service = {
        getByProvider: (provider) =>
          provider === "codex"
            ? Effect.succeed(secondCodex.adapter)
            : Effect.fail(new ProviderUnsupportedError({ provider })),
        listProviders: () => Effect.succeed(["codex"]),
      };
      const secondDirectoryLayer = ProviderSessionDirectoryLive.pipe(
        Layer.provide(runtimeRepositoryLayer),
      );
      const secondProviderLayer = makeProviderServiceLive().pipe(
        Layer.provide(Layer.succeed(ProviderAdapterRegistry, secondRegistry)),
        Layer.provide(secondDirectoryLayer),
        Layer.provide(AnalyticsService.layerTest),
        Layer.provide(ServerSettingsService.layerTest()),
        Layer.provide(ProviderServiceTestSecretStoreLayer),
      );

      yield* Effect.gen(function* () {
        const provider = yield* ProviderService;
        yield* provider.startSession(initial.threadId, {
          provider: "codex",
          threadId: initial.threadId,
          cwd: "/tmp/project-clear-resume",
          runtimeMode: "full-access",
        });
      }).pipe(Effect.provide(secondProviderLayer));

      assert.equal(secondCodex.startSession.mock.calls.length, 1);
      const restartedInput = secondCodex.startSession.mock.calls[0]?.[0];
      assert.equal(typeof restartedInput === "object" && restartedInput !== null, true);
      if (restartedInput && typeof restartedInput === "object") {
        const startPayload = restartedInput as {
          providerOptions?: unknown;
          resumeCursor?: unknown;
        };
        assert.equal(startPayload.providerOptions, undefined);
        assert.equal(startPayload.resumeCursor, undefined);
      }

      fs.rmSync(tempDir, { recursive: true, force: true });
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("recovers provider-instance sessions from current settings after options clear", () =>
    Effect.gen(function* () {
      const tempDir = fs.mkdtempSync(
        path.join(os.tmpdir(), "synara-provider-service-instance-clear-"),
      );
      const dbPath = path.join(tempDir, "orchestration.sqlite");
      const threadId = asThreadId("thread-instance-options-clear");
      const codexHomePath = path.join(tempDir, "codex-work");
      const codexShadowHomePath = path.join(tempDir, "codex-work-shadow");
      const codexEnvironment = { SYNARA_HOME: path.join(tempDir, "synara-runtime") };
      const codexInstanceEnvironment = [
        { name: "SYNARA_HOME", value: codexEnvironment.SYNARA_HOME, sensitive: false },
      ];
      fs.mkdirSync(codexHomePath, { recursive: true });
      fs.mkdirSync(codexShadowHomePath, { recursive: true });
      fs.writeFileSync(path.join(codexHomePath, "config.toml"), "", "utf8");
      fs.writeFileSync(path.join(codexShadowHomePath, "auth.json"), "{}", "utf8");
      buildCodexProcessEnv({
        env: { ...process.env, ...codexEnvironment },
        homePath: codexHomePath,
        shadowHomePath: codexShadowHomePath,
        accountId: "work",
      });
      buildCodexProcessEnv({
        env: { ...process.env, ...codexEnvironment },
        homePath: codexHomePath,
        accountId: "codex_work",
      });
      const persistenceLayer = makeSqlitePersistenceLive(dbPath);
      const runtimeRepositoryLayer = ProviderSessionRuntimeRepositoryLive.pipe(
        Layer.provide(persistenceLayer),
      );
      const firstSettings: Partial<ServerSettings> = {
        providerInstances: {
          codex_work: {
            driver: "codex",
            enabled: true,
            environment: codexInstanceEnvironment,
            config: {
              homePath: codexHomePath,
              shadowHomePath: codexShadowHomePath,
              accountId: "work",
            },
          },
        },
      };
      const secondSettings: Partial<ServerSettings> = {
        providerInstances: {
          codex_work: {
            driver: "codex",
            enabled: true,
            environment: codexInstanceEnvironment,
            config: {
              homePath: codexHomePath,
            },
          },
        },
      };

      const firstCodex = makeFakeCodexAdapter("codex");
      const firstRegistry: typeof ProviderAdapterRegistry.Service = {
        getByProvider: (provider) =>
          provider === "codex"
            ? Effect.succeed(firstCodex.adapter)
            : Effect.fail(new ProviderUnsupportedError({ provider })),
        listProviders: () => Effect.succeed(["codex"]),
      };
      const firstDirectoryLayer = ProviderSessionDirectoryLive.pipe(
        Layer.provide(runtimeRepositoryLayer),
      );
      const firstProviderLayer = makeProviderServiceLive().pipe(
        Layer.provide(Layer.succeed(ProviderAdapterRegistry, firstRegistry)),
        Layer.provide(firstDirectoryLayer),
        Layer.provide(AnalyticsService.layerTest),
        Layer.provide(ServerSettingsService.layerTest(firstSettings)),
        Layer.provide(ProviderServiceTestSecretStoreLayer),
      );

      const initial = yield* Effect.gen(function* () {
        const provider = yield* ProviderService;
        return yield* provider.startSession(threadId, {
          provider: "codex",
          providerInstanceId: "codex_work",
          threadId,
          cwd: "/tmp/project-instance-options-clear",
          runtimeMode: "full-access",
        });
      }).pipe(Effect.provide(firstProviderLayer));

      const secondCodex = makeFakeCodexAdapter("codex");
      const secondRegistry: typeof ProviderAdapterRegistry.Service = {
        getByProvider: (provider) =>
          provider === "codex"
            ? Effect.succeed(secondCodex.adapter)
            : Effect.fail(new ProviderUnsupportedError({ provider })),
        listProviders: () => Effect.succeed(["codex"]),
      };
      const secondDirectoryLayer = ProviderSessionDirectoryLive.pipe(
        Layer.provide(runtimeRepositoryLayer),
      );
      const secondProviderLayer = makeProviderServiceLive().pipe(
        Layer.provide(Layer.succeed(ProviderAdapterRegistry, secondRegistry)),
        Layer.provide(secondDirectoryLayer),
        Layer.provide(AnalyticsService.layerTest),
        Layer.provide(ServerSettingsService.layerTest(secondSettings)),
        Layer.provide(ProviderServiceTestSecretStoreLayer),
      );

      yield* Effect.gen(function* () {
        const provider = yield* ProviderService;
        yield* provider.sendTurn({
          threadId: initial.threadId,
          input: "continue after account fields cleared",
          attachments: [],
        });
      }).pipe(Effect.provide(secondProviderLayer));

      assert.equal(secondCodex.startSession.mock.calls.length, 1);
      const recoveredInput = secondCodex.startSession.mock.calls[0]?.[0];
      assert.equal(typeof recoveredInput === "object" && recoveredInput !== null, true);
      if (recoveredInput && typeof recoveredInput === "object") {
        const startPayload = recoveredInput as {
          providerOptions?: unknown;
          resumeCursor?: unknown;
          providerInstanceId?: string;
        };
        assert.equal(startPayload.providerInstanceId, "codex_work");
        assert.deepEqual(startPayload.providerOptions, {
          codex: {
            homePath: codexHomePath,
            accountId: "codex_work",
            environment: codexEnvironment,
          },
        });
        assert.deepEqual(startPayload.resumeCursor, initial.resumeCursor);
      }

      fs.rmSync(tempDir, { recursive: true, force: true });
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("reuses resume cursor when hydrated instance options only add redacted fields", () =>
    Effect.gen(function* () {
      const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "synara-provider-service-redacted-"));
      const dbPath = path.join(tempDir, "orchestration.sqlite");
      const threadId = asThreadId("thread-redacted-instance-options");
      const persistenceLayer = makeSqlitePersistenceLive(dbPath);
      const runtimeRepositoryLayer = ProviderSessionRuntimeRepositoryLive.pipe(
        Layer.provide(persistenceLayer),
      );
      const settings: Partial<ServerSettings> = {
        providerInstances: {
          opencode_work: {
            driver: "opencode",
            enabled: true,
            environment: [
              { name: "OPENCODE_API_KEY", value: "opencode-env-secret", sensitive: true },
            ],
            config: {
              serverUrl: "http://127.0.0.1:4096",
              serverPassword: "opencode-password",
            },
          },
        },
      };

      const firstOpenCode = makeFakeCodexAdapter("opencode");
      const firstRegistry: typeof ProviderAdapterRegistry.Service = {
        getByProvider: (provider) =>
          provider === "opencode"
            ? Effect.succeed(firstOpenCode.adapter)
            : Effect.fail(new ProviderUnsupportedError({ provider })),
        listProviders: () => Effect.succeed(["opencode"]),
      };
      const firstDirectoryLayer = ProviderSessionDirectoryLive.pipe(
        Layer.provide(runtimeRepositoryLayer),
      );
      const firstProviderLayer = makeProviderServiceLive().pipe(
        Layer.provide(Layer.succeed(ProviderAdapterRegistry, firstRegistry)),
        Layer.provide(firstDirectoryLayer),
        Layer.provide(AnalyticsService.layerTest),
        Layer.provide(ServerSettingsService.layerTest(settings)),
        Layer.provide(ProviderServiceTestSecretStoreLayer),
      );

      const initial = yield* Effect.gen(function* () {
        const provider = yield* ProviderService;
        return yield* provider.startSession(threadId, {
          provider: "opencode",
          providerInstanceId: "opencode_work",
          threadId,
          cwd: "/tmp/project-redacted-instance-options",
          runtimeMode: "full-access",
        });
      }).pipe(Effect.provide(firstProviderLayer));

      const persisted = yield* Effect.gen(function* () {
        const runtimeRepository = yield* ProviderSessionRuntimeRepository;
        return yield* runtimeRepository.getByThreadId({ threadId });
      }).pipe(Effect.provide(runtimeRepositoryLayer));
      assert.equal(Option.isSome(persisted), true);
      if (Option.isSome(persisted)) {
        const runtimePayload = persisted.value.runtimePayload as {
          providerOptionsCredentialsFingerprint?: unknown;
          providerOptions?: {
            opencode?: {
              serverPassword?: string;
              environment?: Record<string, string>;
            };
          };
        };
        const rawSecretFingerprint = createHash("sha256")
          .update(
            JSON.stringify({
              environment: [["OPENCODE_API_KEY", "opencode-env-secret"]],
              serverPassword: "opencode-password",
            }),
          )
          .digest("hex");
        assert.equal(runtimePayload.providerOptions?.opencode?.serverPassword, undefined);
        assert.equal(runtimePayload.providerOptions?.opencode?.environment, undefined);
        assert.equal(typeof runtimePayload.providerOptionsCredentialsFingerprint, "string");
        assert.notEqual(runtimePayload.providerOptionsCredentialsFingerprint, rawSecretFingerprint);
      }

      const secondOpenCode = makeFakeCodexAdapter("opencode");
      const secondRegistry: typeof ProviderAdapterRegistry.Service = {
        getByProvider: (provider) =>
          provider === "opencode"
            ? Effect.succeed(secondOpenCode.adapter)
            : Effect.fail(new ProviderUnsupportedError({ provider })),
        listProviders: () => Effect.succeed(["opencode"]),
      };
      const secondDirectoryLayer = ProviderSessionDirectoryLive.pipe(
        Layer.provide(runtimeRepositoryLayer),
      );
      const secondProviderLayer = makeProviderServiceLive().pipe(
        Layer.provide(Layer.succeed(ProviderAdapterRegistry, secondRegistry)),
        Layer.provide(secondDirectoryLayer),
        Layer.provide(AnalyticsService.layerTest),
        Layer.provide(ServerSettingsService.layerTest(settings)),
        Layer.provide(ProviderServiceTestSecretStoreLayer),
      );

      yield* Effect.gen(function* () {
        const provider = yield* ProviderService;
        yield* provider.startSession(threadId, {
          provider: "opencode",
          providerInstanceId: "opencode_work",
          threadId,
          cwd: "/tmp/project-redacted-instance-options",
          runtimeMode: "full-access",
        });
      }).pipe(Effect.provide(secondProviderLayer));

      assert.equal(secondOpenCode.startSession.mock.calls.length, 1);
      const resumedInput = secondOpenCode.startSession.mock.calls[0]?.[0];
      assert.equal(typeof resumedInput === "object" && resumedInput !== null, true);
      if (resumedInput && typeof resumedInput === "object") {
        const startPayload = resumedInput as {
          providerOptions?: {
            opencode?: {
              serverUrl?: string;
              serverPassword?: string;
              environment?: Record<string, string>;
            };
          };
          resumeCursor?: unknown;
        };
        assert.deepEqual(startPayload.resumeCursor, initial.resumeCursor);
        assert.deepEqual(startPayload.providerOptions?.opencode, {
          serverUrl: "http://127.0.0.1:4096",
          serverPassword: "opencode-password",
          environment: { OPENCODE_API_KEY: "opencode-env-secret" },
        });
      }

      fs.rmSync(tempDir, { recursive: true, force: true });
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("rejects stopped Claude continuation when launch options are removed", () =>
    Effect.gen(function* () {
      const tempDir = fs.mkdtempSync(
        path.join(os.tmpdir(), "synara-provider-service-stop-runtime-"),
      );
      const dbPath = path.join(tempDir, "orchestration.sqlite");
      const persistenceLayer = makeSqlitePersistenceLive(dbPath);
      const runtimeRepositoryLayer = ProviderSessionRuntimeRepositoryLive.pipe(
        Layer.provide(persistenceLayer),
      );
      const providerOptions = {
        claudeAgent: {
          binaryPath: "/usr/local/bin/claude",
          permissionMode: "acceptEdits",
        },
      };

      const firstClaude = makeFakeCodexAdapter("claudeAgent");
      const firstRegistry: typeof ProviderAdapterRegistry.Service = {
        getByProvider: (provider) =>
          provider === "claudeAgent"
            ? Effect.succeed(firstClaude.adapter)
            : Effect.fail(new ProviderUnsupportedError({ provider })),
        listProviders: () => Effect.succeed(["claudeAgent"]),
      };
      const firstDirectoryLayer = ProviderSessionDirectoryLive.pipe(
        Layer.provide(runtimeRepositoryLayer),
      );
      const firstProviderLayer = makeProviderServiceLive().pipe(
        Layer.provide(Layer.succeed(ProviderAdapterRegistry, firstRegistry)),
        Layer.provide(firstDirectoryLayer),
        Layer.provide(AnalyticsService.layerTest),
        Layer.provide(ServerSettingsService.layerTest()),
        Layer.provide(ProviderServiceTestSecretStoreLayer),
      );

      const initial = yield* Effect.gen(function* () {
        const provider = yield* ProviderService;
        const session = yield* provider.startSession(asThreadId("thread-stop-runtime"), {
          provider: "claudeAgent",
          threadId: asThreadId("thread-stop-runtime"),
          cwd: "/tmp/project-stop-runtime",
          providerOptions,
          runtimeMode: "full-access",
        });
        assert.equal(typeof provider.stopRuntimeSession, "function");
        if (provider.stopRuntimeSession) {
          yield* provider.stopRuntimeSession({ threadId: session.threadId });
        }
        return session;
      }).pipe(Effect.provide(firstProviderLayer));

      assert.equal(firstClaude.stopSession.mock.calls.length, 1);

      const secondClaude = makeFakeCodexAdapter("claudeAgent");
      const secondRegistry: typeof ProviderAdapterRegistry.Service = {
        getByProvider: (provider) =>
          provider === "claudeAgent"
            ? Effect.succeed(secondClaude.adapter)
            : Effect.fail(new ProviderUnsupportedError({ provider })),
        listProviders: () => Effect.succeed(["claudeAgent"]),
      };
      const secondDirectoryLayer = ProviderSessionDirectoryLive.pipe(
        Layer.provide(runtimeRepositoryLayer),
      );
      const secondProviderLayer = makeProviderServiceLive().pipe(
        Layer.provide(Layer.succeed(ProviderAdapterRegistry, secondRegistry)),
        Layer.provide(secondDirectoryLayer),
        Layer.provide(AnalyticsService.layerTest),
        Layer.provide(ServerSettingsService.layerTest()),
        Layer.provide(ProviderServiceTestSecretStoreLayer),
      );

      const result = yield* Effect.gen(function* () {
        const provider = yield* ProviderService;
        return yield* Effect.result(
          provider.startSession(initial.threadId, {
            provider: "claudeAgent",
            threadId: initial.threadId,
            cwd: "/tmp/project-stop-runtime",
            runtimeMode: "full-access",
          }),
        );
      }).pipe(Effect.provide(secondProviderLayer));

      assert.equal(result._tag, "Failure");
      assert.equal(secondClaude.startSession.mock.calls.length, 0);

      fs.rmSync(tempDir, { recursive: true, force: true });
    }).pipe(Effect.provide(NodeServices.layer)),
  );
});

restartRollbackRouting.layer("ProviderServiceLive restart-based rollback", (it) => {
  it.effect("requires the source lifecycle generation for modern ACP interactions", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService;
      const directory = yield* ProviderSessionDirectory;
      const threadId = asThreadId("thread-droid-interaction-generation");

      yield* provider.startSession(threadId, {
        provider: "droid",
        threadId,
        cwd: "/tmp/project",
        runtimeMode: "approval-required",
      });
      const binding = Option.getOrUndefined(yield* directory.getBinding(threadId));
      const lifecycleGeneration = binding?.lifecycleGeneration;
      assert.equal(typeof lifecycleGeneration, "string");

      const responseCallCount = restartRollbackRouting.droid.respondToRequest.mock.calls.length;
      const missingGeneration = yield* Effect.result(
        provider.respondToRequest({
          threadId,
          requestId: asRequestId("droid-approval-without-generation"),
          decision: "accept",
        }),
      );
      assertFailure(
        missingGeneration,
        new ProviderValidationError({
          operation: "ProviderService.respondToRequest",
          issue:
            "Cannot respond to request 'droid-approval-without-generation' without its provider lifecycle generation.",
        }),
      );
      assert.equal(
        restartRollbackRouting.droid.respondToRequest.mock.calls.length,
        responseCallCount,
      );

      yield* provider.respondToRequest({
        threadId,
        requestId: asRequestId("droid-approval-current-generation"),
        lifecycleGeneration,
        decision: "accept",
      });
      assert.equal(
        restartRollbackRouting.droid.respondToRequest.mock.calls.length,
        responseCallCount + 1,
      );

      yield* provider.stopSession({ threadId });
      restartRollbackRouting.droid.startSession.mockClear();
      restartRollbackRouting.droid.respondToRequest.mockClear();
      restartRollbackRouting.droid.stopSession.mockClear();
    }),
  );

  it.effect("clears Droid's native cursor instead of reporting a fake rewind", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService;
      const directory = yield* ProviderSessionDirectory;
      const threadId = asThreadId("thread-droid-restart-rollback");
      const session = yield* provider.startSession(threadId, {
        provider: "droid",
        threadId,
        cwd: "/tmp/project",
        runtimeMode: "full-access",
      });

      yield* provider.rollbackConversation({ threadId, numTurns: 1 });

      assert.equal(restartRollbackRouting.droid.rollbackThread.mock.calls.length, 0);
      assert.deepEqual(restartRollbackRouting.droid.stopSession.mock.calls, [[session.threadId]]);
      const binding = yield* directory.getBinding(threadId);
      assert.equal(Option.isSome(binding), true);
      if (Option.isSome(binding)) {
        assert.equal(binding.value.status, "stopped");
        assert.equal(binding.value.resumeCursor, null);
      }
    }),
  );
});

piInteractionRouting.layer("ProviderServiceLive Pi interaction generation", (it) => {
  it.effect("requires the source lifecycle generation for modern Pi user input", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService;
      const directory = yield* ProviderSessionDirectory;
      const threadId = asThreadId("thread-pi-interaction-generation");

      yield* provider.startSession(threadId, {
        provider: "pi",
        threadId,
        cwd: "/tmp/project",
        runtimeMode: "approval-required",
      });
      const binding = Option.getOrUndefined(yield* directory.getBinding(threadId));
      const lifecycleGeneration = binding?.lifecycleGeneration;
      assert.equal(typeof lifecycleGeneration, "string");

      const responseCallCount = piInteractionRouting.pi.respondToUserInput.mock.calls.length;
      const missingGeneration = yield* Effect.result(
        provider.respondToUserInput({
          threadId,
          requestId: asRequestId("pi-user-input-without-generation"),
          answers: { answer: "continue" },
        }),
      );
      assertFailure(
        missingGeneration,
        new ProviderValidationError({
          operation: "ProviderService.respondToUserInput",
          issue:
            "Cannot respond to request 'pi-user-input-without-generation' without its provider lifecycle generation.",
        }),
      );
      assert.equal(piInteractionRouting.pi.respondToUserInput.mock.calls.length, responseCallCount);

      yield* provider.respondToUserInput({
        threadId,
        requestId: asRequestId("pi-user-input-current-generation"),
        lifecycleGeneration,
        answers: { answer: "continue" },
      });
      assert.equal(
        piInteractionRouting.pi.respondToUserInput.mock.calls.length,
        responseCallCount + 1,
      );

      yield* provider.stopSession({ threadId });
      piInteractionRouting.pi.startSession.mockClear();
      piInteractionRouting.pi.respondToUserInput.mockClear();
      piInteractionRouting.pi.stopSession.mockClear();
    }),
  );
});

const idleCleanup = makeProviderServiceLayer({ runtimeIdleStopMs: 100 });
idleCleanup.layer("ProviderServiceLive idle cleanup", (it) => {
  it.effect("does not schedule idle cleanup for a stale terminal event", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService;
      const directory = yield* ProviderSessionDirectory;
      const threadId = asThreadId("thread-idle-stale-terminal");
      const olderTurnId = asTurnId("turn-idle-stale-older");
      const newerTurnId = asTurnId("turn-idle-stale-newer");

      yield* provider.startSession(threadId, {
        provider: "codex",
        threadId,
        runtimeMode: "full-access",
      });
      idleCleanup.codex.sendTurn
        .mockImplementationOnce((input) =>
          Effect.succeed({ threadId: input.threadId, turnId: olderTurnId }),
        )
        .mockImplementationOnce((input) =>
          Effect.succeed({ threadId: input.threadId, turnId: newerTurnId }),
        );
      yield* provider.sendTurn({ threadId, input: "older", attachments: [] });
      yield* provider.sendTurn({ threadId, input: "newer", attachments: [] });

      idleCleanup.codex.stopSession.mockClear();
      yield* idleCleanup.codex.waitForRuntimeSubscribers();
      idleCleanup.codex.emit({
        type: "turn.aborted",
        eventId: asEventId("runtime-idle-stale-older-aborted"),
        provider: "codex",
        createdAt: "2026-02-27T00:04:00.000Z",
        threadId,
        turnId: olderTurnId,
        payload: { state: "interrupted" },
      });
      yield* sleep(150);

      const binding = Option.getOrUndefined(yield* directory.getBinding(threadId));
      const runtimePayload = asRuntimePayloadRecord(binding?.runtimePayload);
      assert.equal(binding?.status, "running");
      assert.equal(runtimePayload.activeTurnId, newerTurnId);
      assert.equal(idleCleanup.codex.stopSession.mock.calls.length, 0);

      idleCleanup.codex.emit({
        type: "turn.completed",
        eventId: asEventId("runtime-idle-newer-completed"),
        provider: "codex",
        createdAt: "2026-02-27T00:04:01.000Z",
        threadId,
        turnId: newerTurnId,
        payload: { state: "completed" },
      });
      yield* waitUntil(
        () => idleCleanup.codex.stopSession.mock.calls.length > 0,
        500,
        20,
        "matching terminal idle cleanup",
      );
      assert.deepEqual(idleCleanup.codex.stopSession.mock.calls[0]?.[0], threadId);
      yield* waitUntilEffect(
        () =>
          directory.getBinding(threadId).pipe(
            Effect.map((current) => {
              const currentBinding = Option.getOrUndefined(current);
              const payload = asRuntimePayloadRecord(currentBinding?.runtimePayload);
              return payload.lastRuntimeEvent === "provider.stopRuntimeSession";
            }),
          ),
        500,
        20,
        "matching terminal idle cleanup persistence",
      );
      idleCleanup.codex.stopSession.mockClear();
    }),
  );

  it.effect("ignores an unscoped terminal event while overlapping turns are outstanding", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService;
      const directory = yield* ProviderSessionDirectory;
      const threadId = asThreadId("thread-idle-ambiguous-terminal");
      const firstTurnId = asTurnId("turn-ambiguous-first");
      const secondTurnId = asTurnId("turn-ambiguous-second");

      yield* provider.startSession(threadId, {
        provider: "codex",
        threadId,
        runtimeMode: "full-access",
      });
      idleCleanup.codex.sendTurn
        .mockImplementationOnce((input) =>
          Effect.succeed({ threadId: input.threadId, turnId: firstTurnId }),
        )
        .mockImplementationOnce((input) =>
          Effect.succeed({ threadId: input.threadId, turnId: secondTurnId }),
        );
      yield* provider.sendTurn({ threadId, input: "first", attachments: [] });
      yield* provider.sendTurn({ threadId, input: "second", attachments: [] });

      idleCleanup.codex.stopSession.mockClear();
      yield* idleCleanup.codex.waitForRuntimeSubscribers();
      idleCleanup.codex.emit({
        type: "turn.aborted",
        eventId: asEventId("runtime-ambiguous-terminal"),
        provider: "codex",
        createdAt: "2026-02-27T00:04:00.000Z",
        threadId,
        payload: { state: "interrupted" },
      });
      yield* sleep(150);

      const binding = Option.getOrUndefined(yield* directory.getBinding(threadId));
      const payload = binding?.runtimePayload as Record<string, unknown>;
      assert.equal(binding?.status, "running");
      assert.equal(payload.activeTurnId, secondTurnId);
      assert.equal(idleCleanup.codex.stopSession.mock.calls.length, 0);
    }),
  );

  it.effect(
    "stops idle ready runtime using the persisted cursor when the live snapshot omits it",
    () =>
      Effect.gen(function* () {
        const provider = yield* ProviderService;
        const runtimeRepository = yield* ProviderSessionRuntimeRepository;

        const session = yield* provider.startSession(asThreadId("thread-idle-persisted-cursor"), {
          provider: "codex",
          threadId: asThreadId("thread-idle-persisted-cursor"),
          runtimeMode: "full-access",
        });

        const persistedBefore = yield* runtimeRepository.getByThreadId({
          threadId: session.threadId,
        });
        assert.equal(Option.isSome(persistedBefore), true);
        if (Option.isSome(persistedBefore)) {
          assert.deepEqual(persistedBefore.value.resumeCursor, session.resumeCursor);
        }

        idleCleanup.codex.updateSession(session.threadId, withoutResumeCursor);
        yield* idleCleanup.codex.waitForRuntimeSubscribers();
        idleCleanup.codex.emit({
          type: "turn.completed",
          eventId: asEventId("runtime-idle-persisted-cursor-complete"),
          provider: "codex",
          createdAt: "2026-02-27T00:04:00.000Z",
          threadId: session.threadId,
          payload: { state: "completed" },
        });

        yield* waitUntil(
          () => idleCleanup.codex.stopSession.mock.calls.length > 0,
          500,
          20,
          "idle runtime stop",
        );

        assert.equal(idleCleanup.codex.stopSession.mock.calls.length, 1);
        assert.deepEqual(idleCleanup.codex.stopSession.mock.calls[0]?.[0], session.threadId);

        const persistedAfter = yield* runtimeRepository.getByThreadId({
          threadId: session.threadId,
        });
        assert.equal(Option.isSome(persistedAfter), true);
        if (Option.isSome(persistedAfter)) {
          assert.equal(persistedAfter.value.status, "stopped");
          assert.deepEqual(persistedAfter.value.resumeCursor, session.resumeCursor);
        }
      }),
  );

  it.effect("clears a pending idle stop before dispatching new turn work", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService;
      const runtimeRepository = yield* ProviderSessionRuntimeRepository;
      const threadId = asThreadId("thread-idle-new-turn");

      idleCleanup.codex.stopSession.mockClear();
      const session = yield* provider.startSession(threadId, {
        provider: "codex",
        threadId,
        runtimeMode: "full-access",
      });
      idleCleanup.codex.updateSession(threadId, withoutResumeCursor);
      yield* idleCleanup.codex.waitForRuntimeSubscribers();
      idleCleanup.codex.emit({
        type: "turn.completed",
        eventId: asEventId("runtime-idle-before-new-turn"),
        provider: "codex",
        createdAt: "2026-02-27T00:04:00.000Z",
        threadId,
        payload: { state: "completed" },
      });

      yield* waitUntilEffect(
        () =>
          runtimeRepository.getByThreadId({ threadId }).pipe(
            Effect.map((runtime) => {
              if (Option.isNone(runtime)) {
                return false;
              }
              const payload = runtime.value.runtimePayload;
              return (
                payload !== null &&
                typeof payload === "object" &&
                !Array.isArray(payload) &&
                (payload as Record<string, unknown>).lastRuntimeEvent === "turn.completed"
              );
            }),
          ),
        500,
        20,
        "runtime completion persistence",
      );

      yield* provider.sendTurn({
        threadId: session.threadId,
        input: "new turn before idle stop",
        attachments: [],
      });
      yield* sleep(150);

      assert.equal(idleCleanup.codex.stopSession.mock.calls.length, 0);
    }),
  );

  it.effect("clears a pending idle stop when a runtime turn starts", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService;
      const runtimeRepository = yield* ProviderSessionRuntimeRepository;
      const threadId = asThreadId("thread-idle-runtime-turn-start");

      idleCleanup.codex.stopSession.mockClear();
      const session = yield* provider.startSession(threadId, {
        provider: "codex",
        threadId,
        runtimeMode: "full-access",
      });
      idleCleanup.codex.updateSession(threadId, withoutResumeCursor);
      yield* idleCleanup.codex.waitForRuntimeSubscribers();
      idleCleanup.codex.emit({
        type: "turn.completed",
        eventId: asEventId("runtime-idle-before-runtime-turn-start"),
        provider: "codex",
        createdAt: "2026-02-27T00:04:00.000Z",
        threadId,
        payload: { state: "completed" },
      });

      yield* waitUntilEffect(
        () =>
          runtimeRepository.getByThreadId({ threadId }).pipe(
            Effect.map((runtime) => {
              if (Option.isNone(runtime)) {
                return false;
              }
              const payload = runtime.value.runtimePayload;
              return (
                payload !== null &&
                typeof payload === "object" &&
                !Array.isArray(payload) &&
                (payload as Record<string, unknown>).lastRuntimeEvent === "turn.completed"
              );
            }),
          ),
        500,
        20,
        "runtime completion persistence",
      );

      idleCleanup.codex.emit({
        type: "turn.started",
        eventId: asEventId("runtime-turn-start-clears-idle"),
        provider: "codex",
        createdAt: "2026-02-27T00:04:01.000Z",
        threadId: session.threadId,
        turnId: asTurnId("turn-runtime-clears-idle"),
        payload: { state: "running" },
      });
      yield* sleep(150);

      assert.equal(idleCleanup.codex.stopSession.mock.calls.length, 0);
    }),
  );

  it.effect("keeps lifecycle ownership on the first of two conflicting turn starts", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService;
      const directory = yield* ProviderSessionDirectory;
      const threadId = asThreadId("thread-conflicting-runtime-starts");
      const firstTurnId = asTurnId("turn-conflicting-start-first");
      const secondTurnId = asTurnId("turn-conflicting-start-second");

      yield* provider.startSession(threadId, {
        provider: "codex",
        threadId,
        runtimeMode: "full-access",
      });
      yield* idleCleanup.codex.waitForRuntimeSubscribers();
      idleCleanup.codex.emit({
        type: "turn.started",
        eventId: asEventId("runtime-conflicting-start-first"),
        provider: "codex",
        createdAt: "2026-02-27T00:04:01.000Z",
        threadId,
        turnId: firstTurnId,
        payload: { state: "running" },
      });
      yield* waitUntilEffect(
        () =>
          directory.getBinding(threadId).pipe(
            Effect.map((current) => {
              const binding = Option.getOrUndefined(current);
              const payload = binding?.runtimePayload as Record<string, unknown> | undefined;
              return payload?.activeTurnId === firstTurnId;
            }),
          ),
        500,
        20,
        "first runtime turn start persistence",
      );

      idleCleanup.codex.emit({
        type: "turn.started",
        eventId: asEventId("runtime-conflicting-start-second"),
        provider: "codex",
        createdAt: "2026-02-27T00:04:02.000Z",
        threadId,
        turnId: secondTurnId,
        payload: { state: "running" },
      });
      yield* sleep(50);

      const binding = Option.getOrUndefined(yield* directory.getBinding(threadId));
      const payload = binding?.runtimePayload as Record<string, unknown>;
      assert.equal(binding?.status, "running");
      assert.equal(payload.activeTurnId, firstTurnId);
      assert.equal(payload.lastRuntimeEvent, "turn.started");
    }),
  );

  it.effect("serializes a fired idle stop before starting new turn work", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService;
      const runtimeRepository = yield* ProviderSessionRuntimeRepository;
      const serverSettings = yield* ServerSettingsService;
      const threadId = asThreadId("thread-idle-fired-new-turn");
      const fixture = makeSharedCodexContinuationFixture(["default"]);
      const providerInstanceId = asProviderInstanceId("codex_idle_fired");
      yield* serverSettings.updateSettings({
        providerInstances: {
          codex_idle_fired: providerInstanceForSharedCodexFixture(fixture, "default"),
        },
      });
      let listSessionsStarted = false;
      let releaseListSessions: ReleaseListSessions | undefined;

      idleCleanup.codex.stopSession.mockClear();
      const session = yield* provider.startSession(threadId, {
        provider: "codex",
        providerInstanceId,
        threadId,
        runtimeMode: "full-access",
      });
      const { resumeCursor: _omittedResumeCursor, ...staleReadySession } = session;
      idleCleanup.codex.startSession.mockClear();

      idleCleanup.codex.listSessions
        .mockImplementationOnce(() => Effect.succeed([session]))
        .mockImplementationOnce(() =>
          Effect.promise(
            () =>
              new Promise<ReadonlyArray<ProviderSession>>((resolve) => {
                listSessionsStarted = true;
                releaseListSessions = resolve;
              }),
          ),
        );

      yield* idleCleanup.codex.waitForRuntimeSubscribers();
      idleCleanup.codex.emit({
        type: "turn.completed",
        eventId: asEventId("runtime-idle-fired-before-new-turn"),
        provider: "codex",
        providerInstanceId,
        createdAt: "2026-02-27T00:04:00.000Z",
        threadId,
        payload: { state: "completed" },
      });

      yield* waitUntilEffect(
        () =>
          runtimeRepository.getByThreadId({ threadId }).pipe(
            Effect.map((runtime) => {
              if (Option.isNone(runtime)) {
                return false;
              }
              const payload = runtime.value.runtimePayload;
              return (
                payload !== null &&
                typeof payload === "object" &&
                !Array.isArray(payload) &&
                (payload as Record<string, unknown>).lastRuntimeEvent === "turn.completed"
              );
            }),
          ),
        500,
        20,
        "runtime completion persistence",
      );
      yield* waitUntil(() => listSessionsStarted, 500, 20, "idle listSessions start");

      const sendTurnFiber = yield* provider
        .sendTurn({
          threadId,
          input: "new turn after idle timeout fired",
          attachments: [],
        })
        .pipe(Effect.forkChild);

      const release = releaseListSessions;
      requireReleaseListSessions(release)([staleReadySession]);
      yield* Fiber.join(sendTurnFiber);
      yield* sleep(100);

      assert.equal(idleCleanup.codex.stopSession.mock.calls.length, 1);
      assert.equal(idleCleanup.codex.startSession.mock.calls.length, 1);
      assert.equal(
        idleCleanup.codex.startSession.mock.calls[0]?.[0].expectedCodexContinuationGeneration,
        requireSharedCodexFixtureGeneration(fixture, "default"),
      );
      const persistedAfter = yield* runtimeRepository.getByThreadId({ threadId });
      assert.equal(Option.isSome(persistedAfter), true);
      if (Option.isSome(persistedAfter)) {
        assert.equal(persistedAfter.value.status, "running");
        const payload = persistedAfter.value.runtimePayload;
        assert.equal(
          payload !== null &&
            typeof payload === "object" &&
            !Array.isArray(payload) &&
            (payload as Record<string, unknown>).activeTurnId === `turn-${String(threadId)}`,
          true,
        );
      }
      fs.rmSync(fixture.root, { recursive: true, force: true });
    }),
  );

  it.effect("restores idle cleanup when new turn dispatch is interrupted", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService;
      const runtimeRepository = yield* ProviderSessionRuntimeRepository;
      const threadId = asThreadId("thread-idle-interrupted-dispatch");

      idleCleanup.codex.stopSession.mockClear();
      const session = yield* provider.startSession(threadId, {
        provider: "codex",
        threadId,
        runtimeMode: "full-access",
      });
      idleCleanup.codex.updateSession(threadId, withoutResumeCursor);
      yield* idleCleanup.codex.waitForRuntimeSubscribers();
      idleCleanup.codex.emit({
        type: "turn.completed",
        eventId: asEventId("runtime-idle-before-interrupted-dispatch"),
        provider: "codex",
        createdAt: "2026-02-27T00:04:00.000Z",
        threadId,
        payload: { state: "completed" },
      });

      yield* waitUntilEffect(
        () =>
          runtimeRepository.getByThreadId({ threadId }).pipe(
            Effect.map((runtime) => {
              if (Option.isNone(runtime)) {
                return false;
              }
              const payload = runtime.value.runtimePayload;
              return (
                payload !== null &&
                typeof payload === "object" &&
                !Array.isArray(payload) &&
                (payload as Record<string, unknown>).lastRuntimeEvent === "turn.completed"
              );
            }),
          ),
        500,
        20,
        "runtime completion persistence",
      );

      idleCleanup.codex.sendTurn.mockImplementationOnce(() => Effect.interrupt);
      yield* Effect.exit(
        provider.sendTurn({
          threadId: session.threadId,
          input: "new turn interrupted before runtime events",
          attachments: [],
        }),
      );

      yield* waitUntil(
        () => idleCleanup.codex.stopSession.mock.calls.length > 0,
        500,
        20,
        "idle runtime stop after interrupted dispatch",
      );
      assert.deepEqual(idleCleanup.codex.stopSession.mock.calls[0]?.[0], threadId);
    }),
  );

  it.effect("reschedules idle cleanup after successful rollback work", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService;
      const runtimeRepository = yield* ProviderSessionRuntimeRepository;
      const threadId = asThreadId("thread-idle-rollback-success");

      yield* provider.startSession(threadId, {
        provider: "codex",
        threadId,
        runtimeMode: "full-access",
      });
      idleCleanup.codex.updateSession(threadId, withoutResumeCursor);
      yield* idleCleanup.codex.waitForRuntimeSubscribers();
      idleCleanup.codex.emit({
        type: "turn.completed",
        eventId: asEventId("runtime-idle-before-rollback"),
        provider: "codex",
        createdAt: "2026-02-27T00:04:00.000Z",
        threadId,
        payload: { state: "completed" },
      });

      yield* waitUntilEffect(
        () =>
          runtimeRepository.getByThreadId({ threadId }).pipe(
            Effect.map((runtime) => {
              if (Option.isNone(runtime)) {
                return false;
              }
              const payload = runtime.value.runtimePayload;
              return (
                payload !== null &&
                typeof payload === "object" &&
                !Array.isArray(payload) &&
                (payload as Record<string, unknown>).lastRuntimeEvent === "turn.completed"
              );
            }),
          ),
        500,
        20,
        "runtime completion persistence",
      );

      idleCleanup.codex.stopSession.mockClear();
      yield* provider.rollbackConversation({
        threadId,
        numTurns: 1,
      });

      yield* waitUntil(
        () => idleCleanup.codex.stopSession.mock.calls.length > 0,
        500,
        20,
        "idle runtime stop after successful rollback",
      );
      assert.deepEqual(idleCleanup.codex.stopSession.mock.calls[0]?.[0], threadId);
    }),
  );

  it.effect("waits for fired idle cleanup before removing an explicit stop binding", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService;
      const directory = yield* ProviderSessionDirectory;
      const runtimeRepository = yield* ProviderSessionRuntimeRepository;
      const threadId = asThreadId("thread-idle-stop-remove-race");
      let listSessionsStarted = false;
      let releaseListSessions: ReleaseListSessions | undefined;

      const session = yield* provider.startSession(threadId, {
        provider: "codex",
        threadId,
        runtimeMode: "full-access",
      });
      const { resumeCursor: _omittedResumeCursor, ...staleReadySession } = session;
      idleCleanup.codex.listSessions
        .mockImplementationOnce(() => Effect.succeed([session]))
        .mockImplementationOnce(() =>
          Effect.promise(
            () =>
              new Promise<ReadonlyArray<ProviderSession>>((resolve) => {
                listSessionsStarted = true;
                releaseListSessions = resolve;
              }),
          ),
        );

      yield* idleCleanup.codex.waitForRuntimeSubscribers();
      idleCleanup.codex.emit({
        type: "turn.completed",
        eventId: asEventId("runtime-idle-before-explicit-stop"),
        provider: "codex",
        createdAt: "2026-02-27T00:04:00.000Z",
        threadId,
        payload: { state: "completed" },
      });
      yield* waitUntilEffect(
        () =>
          runtimeRepository.getByThreadId({ threadId }).pipe(
            Effect.map((runtime) => {
              if (Option.isNone(runtime)) {
                return false;
              }
              const payload = runtime.value.runtimePayload;
              return (
                payload !== null &&
                typeof payload === "object" &&
                !Array.isArray(payload) &&
                (payload as Record<string, unknown>).lastRuntimeEvent === "turn.completed"
              );
            }),
          ),
        500,
        20,
        "runtime completion persistence",
      );
      yield* waitUntil(() => listSessionsStarted, 500, 20, "idle listSessions start");

      const stopFiber = yield* provider.stopSession({ threadId }).pipe(Effect.forkChild);
      const release = releaseListSessions;
      requireReleaseListSessions(release)([staleReadySession]);
      yield* Fiber.join(stopFiber);

      const binding = yield* directory.getBinding(threadId);
      assert.equal(Option.isNone(binding), true);
    }),
  );

  it.effect("waits for fired idle cleanup before explicit runtime stop", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService;
      const runtimeRepository = yield* ProviderSessionRuntimeRepository;
      const threadId = asThreadId("thread-idle-runtime-stop-race");
      let listSessionsStarted = false;
      let releaseListSessions: ReleaseListSessions | undefined;

      const session = yield* provider.startSession(threadId, {
        provider: "codex",
        threadId,
        runtimeMode: "full-access",
      });
      const { resumeCursor: _omittedResumeCursor, ...staleReadySession } = session;
      idleCleanup.codex.stopSession.mockClear();
      idleCleanup.codex.listSessions
        .mockImplementationOnce(() => Effect.succeed([session]))
        .mockImplementationOnce(() =>
          Effect.promise(
            () =>
              new Promise<ReadonlyArray<ProviderSession>>((resolve) => {
                listSessionsStarted = true;
                releaseListSessions = resolve;
              }),
          ),
        );

      yield* idleCleanup.codex.waitForRuntimeSubscribers();
      idleCleanup.codex.emit({
        type: "turn.completed",
        eventId: asEventId("runtime-idle-before-runtime-stop"),
        provider: "codex",
        createdAt: "2026-02-27T00:04:00.000Z",
        threadId,
        payload: { state: "completed" },
      });
      yield* waitUntilEffect(
        () =>
          runtimeRepository.getByThreadId({ threadId }).pipe(
            Effect.map((runtime) => {
              if (Option.isNone(runtime)) {
                return false;
              }
              const payload = runtime.value.runtimePayload;
              return (
                payload !== null &&
                typeof payload === "object" &&
                !Array.isArray(payload) &&
                (payload as Record<string, unknown>).lastRuntimeEvent === "turn.completed"
              );
            }),
          ),
        500,
        20,
        "runtime completion persistence",
      );
      yield* waitUntil(() => listSessionsStarted, 500, 20, "idle listSessions start");

      assert.equal(typeof provider.stopRuntimeSession, "function");
      if (!provider.stopRuntimeSession) {
        assert.fail("stopRuntimeSession unavailable");
      }
      const stopFiber = yield* provider.stopRuntimeSession({ threadId }).pipe(Effect.forkChild);
      const release = releaseListSessions;
      requireReleaseListSessions(release)([staleReadySession]);
      yield* Fiber.join(stopFiber);

      assert.equal(idleCleanup.codex.stopSession.mock.calls.length, 1);
      assert.deepEqual(idleCleanup.codex.stopSession.mock.calls[0]?.[0], threadId);
    }),
  );

  it.effect("reschedules idle cleanup after successful compact work", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService;
      const runtimeRepository = yield* ProviderSessionRuntimeRepository;
      const threadId = asThreadId("thread-idle-compact-success");

      yield* provider.startSession(threadId, {
        provider: "codex",
        threadId,
        runtimeMode: "full-access",
      });
      idleCleanup.codex.updateSession(threadId, withoutResumeCursor);
      yield* idleCleanup.codex.waitForRuntimeSubscribers();
      idleCleanup.codex.emit({
        type: "turn.completed",
        eventId: asEventId("runtime-idle-before-compact-success"),
        provider: "codex",
        createdAt: "2026-02-27T00:04:00.000Z",
        threadId,
        payload: { state: "completed" },
      });

      yield* waitUntilEffect(
        () =>
          runtimeRepository.getByThreadId({ threadId }).pipe(
            Effect.map((runtime) => {
              if (Option.isNone(runtime)) {
                return false;
              }
              const payload = runtime.value.runtimePayload;
              return (
                payload !== null &&
                typeof payload === "object" &&
                !Array.isArray(payload) &&
                (payload as Record<string, unknown>).lastRuntimeEvent === "turn.completed"
              );
            }),
          ),
        500,
        20,
        "runtime completion persistence",
      );

      idleCleanup.codex.stopSession.mockClear();
      idleCleanup.codex.compactThread.mockImplementationOnce((inputThreadId) =>
        Effect.sync(() => {
          idleCleanup.codex.updateSession(inputThreadId, (existing) => ({
            ...existing,
            status: "running",
            activeTurnId: undefined,
          }));
        }),
      );
      yield* provider.compactThread({ threadId });

      yield* waitUntil(
        () => idleCleanup.codex.stopSession.mock.calls.length > 0,
        500,
        20,
        "idle runtime stop after successful compact",
      );
      assert.deepEqual(idleCleanup.codex.stopSession.mock.calls[0]?.[0], threadId);
    }),
  );

  it.effect("schedules idle cleanup for closed thread state changes", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService;
      const threadId = asThreadId("thread-idle-closed-state");

      yield* provider.startSession(threadId, {
        provider: "codex",
        threadId,
        runtimeMode: "full-access",
      });
      idleCleanup.codex.stopSession.mockClear();
      yield* idleCleanup.codex.waitForRuntimeSubscribers();
      idleCleanup.codex.emit({
        type: "thread.state.changed",
        eventId: asEventId("runtime-idle-closed-state"),
        provider: "codex",
        createdAt: "2026-02-27T00:04:00.000Z",
        threadId,
        payload: { state: "closed" },
      });

      yield* waitUntil(
        () => idleCleanup.codex.stopSession.mock.calls.length > 0,
        500,
        20,
        "idle runtime stop after closed thread state",
      );
      assert.deepEqual(idleCleanup.codex.stopSession.mock.calls[0]?.[0], threadId);
    }),
  );

  it.effect("stops a compacted runtime that remains running without an active turn", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService;
      const threadId = asThreadId("thread-idle-compact-running");

      yield* provider.startSession(threadId, {
        provider: "codex",
        threadId,
        runtimeMode: "full-access",
      });
      idleCleanup.codex.stopSession.mockClear();
      idleCleanup.codex.updateSession(threadId, (existing) => ({
        ...existing,
        status: "running",
        activeTurnId: undefined,
      }));
      yield* idleCleanup.codex.waitForRuntimeSubscribers();
      idleCleanup.codex.emit({
        type: "thread.state.changed",
        eventId: asEventId("runtime-idle-compact-completed"),
        provider: "codex",
        createdAt: "2026-02-27T00:04:00.000Z",
        threadId,
        payload: { state: "compacted" },
      });

      yield* waitUntil(
        () => idleCleanup.codex.stopSession.mock.calls.length > 0,
        500,
        20,
        "idle runtime stop after compact",
      );
      assert.deepEqual(idleCleanup.codex.stopSession.mock.calls[0]?.[0], threadId);
    }),
  );

  it.effect("restores idle cleanup when new turn dispatch fails before runtime events", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService;
      const runtimeRepository = yield* ProviderSessionRuntimeRepository;
      const threadId = asThreadId("thread-idle-failed-dispatch");
      const dispatchFailure = new ProviderAdapterSessionNotFoundError({
        provider: "codex",
        threadId,
      });

      idleCleanup.codex.stopSession.mockClear();
      const session = yield* provider.startSession(threadId, {
        provider: "codex",
        threadId,
        runtimeMode: "full-access",
      });
      idleCleanup.codex.updateSession(threadId, withoutResumeCursor);
      yield* idleCleanup.codex.waitForRuntimeSubscribers();
      idleCleanup.codex.emit({
        type: "turn.completed",
        eventId: asEventId("runtime-idle-before-failed-dispatch"),
        provider: "codex",
        createdAt: "2026-02-27T00:04:00.000Z",
        threadId,
        payload: { state: "completed" },
      });

      yield* waitUntilEffect(
        () =>
          runtimeRepository.getByThreadId({ threadId }).pipe(
            Effect.map((runtime) => {
              if (Option.isNone(runtime)) {
                return false;
              }
              const payload = runtime.value.runtimePayload;
              return (
                payload !== null &&
                typeof payload === "object" &&
                !Array.isArray(payload) &&
                (payload as Record<string, unknown>).lastRuntimeEvent === "turn.completed"
              );
            }),
          ),
        500,
        20,
        "runtime completion persistence",
      );

      idleCleanup.codex.sendTurn.mockImplementationOnce(() => Effect.fail(dispatchFailure));
      const failedTurn = yield* Effect.result(
        provider.sendTurn({
          threadId: session.threadId,
          input: "new turn that fails before runtime events",
          attachments: [],
        }),
      );
      assertFailure(failedTurn, dispatchFailure);

      yield* waitUntil(
        () => idleCleanup.codex.stopSession.mock.calls.length > 0,
        500,
        20,
        "idle runtime stop after failed dispatch",
      );
      assert.deepEqual(idleCleanup.codex.stopSession.mock.calls[0]?.[0], threadId);
    }),
  );
});

const fanout = makeProviderServiceLayer();
fanout.layer("ProviderServiceLive fanout", (it) => {
  it.effect("fans out adapter turn completion events", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService;
      const session = yield* provider.startSession(asThreadId("thread-1"), {
        provider: "codex",
        threadId: asThreadId("thread-1"),
        runtimeMode: "full-access",
      });

      const eventsRef = yield* Ref.make<Array<ProviderRuntimeEvent>>([]);
      const consumer = yield* Stream.runForEach(provider.streamEvents, (event) =>
        Ref.update(eventsRef, (current) => [...current, event]),
      ).pipe(Effect.forkChild);
      yield* sleep(50);

      const completedEvent: LegacyProviderRuntimeEvent = {
        type: "turn.completed",
        eventId: asEventId("evt-1"),
        provider: "codex",
        createdAt: new Date().toISOString(),
        threadId: session.threadId,
        turnId: asTurnId("turn-1"),
        status: "completed",
      };

      fanout.codex.emit(completedEvent);
      yield* sleep(50);

      const events = yield* Ref.get(eventsRef);
      yield* Fiber.interrupt(consumer);

      assert.equal(
        events.some((entry) => entry.type === "turn.completed"),
        true,
      );
    }),
  );

  it.effect("fans out canonical runtime events in emission order", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService;
      const session = yield* provider.startSession(asThreadId("thread-seq"), {
        provider: "codex",
        threadId: asThreadId("thread-seq"),
        runtimeMode: "full-access",
      });

      const receivedRef = yield* Ref.make<Array<ProviderRuntimeEvent>>([]);
      const consumer = yield* Stream.take(provider.streamEvents, 3).pipe(
        Stream.runForEach((event) => Ref.update(receivedRef, (current) => [...current, event])),
        Effect.forkChild,
      );
      yield* sleep(50);

      fanout.codex.emit({
        type: "tool.started",
        eventId: asEventId("evt-seq-1"),
        provider: "codex",
        createdAt: new Date().toISOString(),
        threadId: session.threadId,
        turnId: asTurnId("turn-1"),
        toolKind: "command",
        title: "Ran command",
      });
      fanout.codex.emit({
        type: "tool.completed",
        eventId: asEventId("evt-seq-2"),
        provider: "codex",
        createdAt: new Date().toISOString(),
        threadId: session.threadId,
        turnId: asTurnId("turn-1"),
        toolKind: "command",
        title: "Ran command",
      });
      fanout.codex.emit({
        type: "turn.completed",
        eventId: asEventId("evt-seq-3"),
        provider: "codex",
        createdAt: new Date().toISOString(),
        threadId: session.threadId,
        turnId: asTurnId("turn-1"),
        status: "completed",
      });

      yield* Fiber.join(consumer);
      const received = yield* Ref.get(receivedRef);
      assert.deepEqual(
        received.map((event) => event.eventId),
        [asEventId("evt-seq-1"), asEventId("evt-seq-2"), asEventId("evt-seq-3")],
      );
    }),
  );

  it.effect("keeps subscriber delivery ordered and isolates failing subscribers", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService;
      const threadId = asThreadId("thread-ordered-subscriber-delivery");
      const session = yield* provider.startSession(threadId, {
        provider: "codex",
        threadId,
        runtimeMode: "full-access",
      });

      const receivedByHealthy: string[] = [];
      const expectedEventIds = new Set<string>(["evt-ordered-1", "evt-ordered-2", "evt-ordered-3"]);
      const healthyFiber = yield* Stream.take(provider.streamEvents, 3).pipe(
        Stream.runForEach((event) =>
          Effect.sync(() => {
            receivedByHealthy.push(event.eventId);
          }),
        ),
        Effect.forkChild,
      );
      const failingFiber = yield* Stream.take(provider.streamEvents, 1).pipe(
        Stream.runForEach(() => Effect.fail("listener crash")),
        Effect.forkChild,
      );
      yield* sleep(50);

      const events: ReadonlyArray<LegacyProviderRuntimeEvent> = [
        {
          type: "tool.completed",
          eventId: asEventId("evt-ordered-1"),
          provider: "codex",
          createdAt: new Date().toISOString(),
          threadId: session.threadId,
          turnId: asTurnId("turn-1"),
          toolKind: "command",
          title: "Ran command",
          detail: "echo one",
        },
        {
          type: "message.delta",
          eventId: asEventId("evt-ordered-2"),
          provider: "codex",
          createdAt: new Date().toISOString(),
          threadId: session.threadId,
          turnId: asTurnId("turn-1"),
          delta: "hello",
        },
        {
          type: "turn.completed",
          eventId: asEventId("evt-ordered-3"),
          provider: "codex",
          createdAt: new Date().toISOString(),
          threadId: session.threadId,
          turnId: asTurnId("turn-1"),
          status: "completed",
        },
      ];

      for (const event of events) {
        fanout.codex.emit(event);
      }
      const failingResult = yield* Effect.result(Fiber.join(failingFiber));
      assert.equal(failingResult._tag, "Failure");
      yield* Fiber.join(healthyFiber);

      assert.deepEqual(
        receivedByHealthy.filter((eventId) => expectedEventIds.has(eventId)).slice(0, 3),
        ["evt-ordered-1", "evt-ordered-2", "evt-ordered-3"],
      );
    }),
  );

  it.effect("clears persisted active turn when provider session reports ready", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService;
      const directory = yield* ProviderSessionDirectory;
      const threadId = asThreadId("thread-ready");
      yield* provider.startSession(threadId, {
        provider: "codex",
        threadId,
        runtimeMode: "full-access",
      });
      yield* provider.sendTurn({ threadId, input: "hello" });
      yield* sleep(50);

      fanout.codex.emit({
        type: "session.state.changed",
        eventId: asEventId("evt-ready"),
        provider: "codex",
        createdAt: new Date().toISOString(),
        threadId,
        payload: {
          state: "ready",
        },
      });
      yield* sleep(50);

      const binding = Option.getOrUndefined(yield* directory.getBinding(threadId));
      const runtimePayload = asRuntimePayloadRecord(binding?.runtimePayload);
      assert.equal(runtimePayload.activeTurnId, null);
    }),
  );
});

const instanceEventFanout = makeProviderServiceLayer(undefined, {
  providerInstances: {
    codex_a: { driver: "codex", enabled: true },
    codex_b: { driver: "codex", enabled: true },
  },
});

instanceEventFanout.layer("ProviderServiceLive runtime event instance correlation", (it) => {
  it.effect("preserves B identity on startup events emitted before binding persistence", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService;
      const directory = yield* ProviderSessionDirectory;
      const threadId = asThreadId("thread-instance-startup-b");
      const providerInstanceId = asProviderInstanceId("codex_b");
      const startupEventId = asEventId("evt-instance-startup-b");
      const receivedRef = yield* Ref.make<Array<ProviderRuntimeEvent>>([]);
      const consumer = yield* Stream.take(
        Stream.filter(provider.streamEvents, (event) => event.eventId === startupEventId),
        1,
      ).pipe(
        Stream.runForEach((event) => Ref.update(receivedRef, (current) => [...current, event])),
        Effect.forkChild,
      );
      yield* sleep(20);

      instanceEventFanout.codex.startSession.mockImplementationOnce(
        (input: ProviderSessionStartInput) =>
          Effect.gen(function* () {
            const now = new Date().toISOString();
            instanceEventFanout.codex.emit({
              type: "session.started",
              eventId: startupEventId,
              provider: "codex",
              providerInstanceId: input.providerInstanceId,
              createdAt: now,
              threadId: input.threadId,
              payload: {},
            });
            yield* waitUntilEffect(
              () =>
                Ref.get(receivedRef).pipe(
                  Effect.map((events) => events.some((event) => event.eventId === startupEventId)),
                ),
              500,
              20,
              "startup event delivery before binding persistence",
            );
            const bindingBeforePersistence = yield* directory
              .getBinding(input.threadId)
              .pipe(Effect.orElseSucceed(() => Option.none()));
            assert.equal(Option.isNone(bindingBeforePersistence), true);
            return {
              provider: "codex",
              ...(input.providerInstanceId ? { providerInstanceId: input.providerInstanceId } : {}),
              status: "ready",
              runtimeMode: input.runtimeMode,
              threadId: input.threadId,
              cwd: input.cwd ?? process.cwd(),
              createdAt: now,
              updatedAt: now,
            } satisfies ProviderSession;
          }),
      );

      const session = yield* provider.startSession(threadId, {
        provider: "codex",
        providerInstanceId,
        threadId,
        runtimeMode: "full-access",
      });
      yield* Fiber.join(consumer);

      const received = yield* Ref.get(receivedRef);
      assert.equal(received.length, 1);
      assert.equal(received[0]?.providerInstanceId, providerInstanceId);
      assert.equal(session.providerInstanceId, providerInstanceId);
      const binding = Option.getOrUndefined(yield* directory.getBinding(threadId));
      assert.equal(binding?.providerInstanceId, providerInstanceId);
    }),
  );

  it.effect("drops delayed A events after the binding switches to B", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService;
      const directory = yield* ProviderSessionDirectory;
      const threadId = asThreadId("thread-instance-delayed-a");
      const instanceA = asProviderInstanceId("codex_a");
      const instanceB = asProviderInstanceId("codex_b");
      const staleEventId = asEventId("evt-instance-delayed-a");
      const currentEventId = asEventId("evt-instance-current-b");

      yield* directory.upsert({
        threadId,
        provider: "codex",
        providerInstanceId: instanceA,
        runtimeMode: "full-access",
        status: "running",
      });
      yield* directory.upsert({
        threadId,
        provider: "codex",
        providerInstanceId: instanceB,
        runtimeMode: "full-access",
        status: "running",
      });

      const receivedRef = yield* Ref.make<Array<ProviderRuntimeEvent>>([]);
      const eventIds = new Set<string>([staleEventId, currentEventId]);
      const consumer = yield* Stream.take(
        Stream.filter(provider.streamEvents, (event) => eventIds.has(event.eventId)),
        1,
      ).pipe(
        Stream.runForEach((event) => Ref.update(receivedRef, (current) => [...current, event])),
        Effect.forkChild,
      );
      yield* sleep(20);

      instanceEventFanout.codex.emit({
        type: "session.state.changed",
        eventId: staleEventId,
        provider: "codex",
        providerInstanceId: instanceA,
        createdAt: "2026-07-11T10:00:00.000Z",
        threadId,
        payload: { state: "ready" },
      });
      instanceEventFanout.codex.emit({
        type: "session.state.changed",
        eventId: currentEventId,
        provider: "codex",
        providerInstanceId: instanceB,
        createdAt: "2026-07-11T10:00:01.000Z",
        threadId,
        payload: { state: "ready" },
      });

      yield* Fiber.join(consumer);
      const received = yield* Ref.get(receivedRef);
      assert.deepEqual(
        received.map((event) => [event.eventId, event.providerInstanceId]),
        [[currentEventId, instanceB]],
      );
    }),
  );
});

const validation = makeProviderServiceLayer();
validation.layer("ProviderServiceLive validation", (it) => {
  it.effect("returns ProviderValidationError for invalid input payloads", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService;

      const failure = yield* Effect.result(
        provider.startSession(asThreadId("thread-validation"), {
          threadId: asThreadId("thread-validation"),
          provider: "invalid-provider",
          runtimeMode: "full-access",
        } as never),
      );

      assert.equal(failure._tag, "Failure");
      if (failure._tag !== "Failure") {
        return;
      }
      assert.equal(failure.failure._tag, "ProviderValidationError");
      if (failure.failure._tag !== "ProviderValidationError") {
        return;
      }
      assert.equal(failure.failure.operation, "ProviderService.startSession");
      assert.equal(failure.failure.issue.includes("invalid-provider"), true);
    }),
  );

  it.effect("accepts startSession when adapter has not emitted provider thread id yet", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService;
      const runtimeRepository = yield* ProviderSessionRuntimeRepository;

      validation.codex.startSession.mockImplementationOnce((input: ProviderSessionStartInput) =>
        Effect.sync(() => {
          const now = new Date().toISOString();
          return {
            provider: "codex",
            status: "ready",
            threadId: input.threadId,
            runtimeMode: input.runtimeMode,
            cwd: input.cwd ?? process.cwd(),
            createdAt: now,
            updatedAt: now,
          } satisfies ProviderSession;
        }),
      );

      const session = yield* provider.startSession(asThreadId("thread-missing"), {
        provider: "codex",
        threadId: asThreadId("thread-missing"),
        cwd: "/tmp/project",
        runtimeMode: "full-access",
      });

      assert.equal(session.threadId, asThreadId("thread-missing"));

      const runtime = yield* runtimeRepository.getByThreadId({
        threadId: session.threadId,
      });
      assert.equal(Option.isSome(runtime), true);
      if (Option.isSome(runtime)) {
        assert.equal(runtime.value.threadId, session.threadId);
      }
    }),
  );
});

const boundedFanout = makeProviderServiceLayer({ runtimeEventBufferCapacity: 1 });
it.effect("ProviderServiceLive backpressures slow subscribers and completes fanout shutdown", () =>
  Effect.gen(function* () {
    const scope = yield* Scope.make("sequential");
    const releaseSlowConsumer = yield* Deferred.make<void>();
    yield* Effect.gen(function* () {
      const services = yield* Layer.buildWithScope(boundedFanout.rawLayer, scope);
      const provider = yield* Effect.service(ProviderService).pipe(Effect.provide(services));
      const threadId = asThreadId("thread-bounded-fanout");
      yield* provider.startSession(threadId, {
        provider: "codex",
        threadId,
        runtimeMode: "full-access",
      });
      yield* boundedFanout.codex.waitForRuntimeSubscribers();

      const slowConsumerStarted = yield* Deferred.make<void>();
      const slowConsumer = yield* Stream.runForEach(provider.streamEvents, () =>
        Deferred.succeed(slowConsumerStarted, undefined).pipe(
          Effect.andThen(Deferred.await(releaseSlowConsumer)),
        ),
      ).pipe(Effect.forkChild);

      const receivedByHealthy = yield* Ref.make<Array<string>>([]);
      const healthyConsumer = yield* Stream.take(provider.streamEvents, 3).pipe(
        Stream.runForEach((event) =>
          Ref.update(receivedByHealthy, (current) => [...current, event.eventId]),
        ),
        Effect.forkChild,
      );
      yield* sleep(20);

      for (const index of [1, 2, 3]) {
        boundedFanout.codex.emit({
          type: "message.delta",
          eventId: asEventId(`evt-bounded-${index}`),
          provider: "codex",
          createdAt: new Date().toISOString(),
          threadId,
          turnId: asTurnId("turn-bounded"),
          delta: String(index),
        });
      }

      yield* Deferred.await(slowConsumerStarted);
      yield* sleep(30);
      const receivedBeforeRelease = yield* Ref.get(receivedByHealthy);
      yield* Deferred.succeed(releaseSlowConsumer, undefined);
      assert.equal(receivedBeforeRelease.length < 3, true);
      yield* Fiber.join(healthyConsumer);
      assert.deepEqual(yield* Ref.get(receivedByHealthy), [
        asEventId("evt-bounded-1"),
        asEventId("evt-bounded-2"),
        asEventId("evt-bounded-3"),
      ]);

      yield* provider.closeRuntimeEvents;
      yield* provider.closeRuntimeEvents;
      yield* Fiber.interrupt(slowConsumer);
    }).pipe(
      Effect.ensuring(Deferred.succeed(releaseSlowConsumer, undefined).pipe(Effect.asVoid)),
      Effect.ensuring(Scope.close(scope, Exit.void)),
    );
  }),
);

const liveFallback = makeProviderServiceLayer();
liveFallback.layer("ProviderServiceLive live-fallback settled turns", (it) => {
  it.effect("persists the first binding row as stopped when the turn settles pre-write", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService;
      const directory = yield* ProviderSessionDirectory;
      const threadId = asThreadId("thread-live-fallback-settled");
      const turnId = asTurnId("turn-live-fallback-settled");

      // The adapter owns a live session but startSession has not persisted a
      // binding row yet (the startup window resolveRoutableSession allows).
      liveFallback.codex.hasSession.mockImplementation((candidate: ThreadId) =>
        Effect.succeed(candidate === threadId),
      );
      liveFallback.codex.sendTurn.mockImplementationOnce((input: ProviderSendTurnInput) =>
        Effect.gen(function* () {
          // The terminal runtime event is fully processed before sendTurn
          // returns, so the post-dispatch write takes the settled-turn branch.
          liveFallback.codex.emit({
            type: "turn.completed",
            eventId: asEventId("evt-live-fallback-settled"),
            provider: "codex",
            createdAt: new Date().toISOString(),
            threadId: input.threadId,
            turnId,
            payload: { state: "cancelled" },
          });
          yield* sleep(100);
          return { threadId: input.threadId, turnId };
        }),
      );
      yield* liveFallback.codex.waitForRuntimeSubscribers();

      yield* provider.sendTurn({ threadId, input: "hello" });

      const binding = Option.getOrUndefined(yield* directory.getBinding(threadId));
      assert.equal(binding?.status, "stopped");
    }),
  );

  it.effect("retains settlement markers for more than eight overlapping dispatches", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService;
      const directory = yield* ProviderSessionDirectory;
      const threadId = asThreadId("thread-live-fallback-many-settled");
      let sequence = 0;

      liveFallback.codex.hasSession.mockImplementation((candidate: ThreadId) =>
        Effect.succeed(candidate === threadId),
      );
      liveFallback.codex.sendTurn.mockImplementation((input: ProviderSendTurnInput) =>
        Effect.gen(function* () {
          sequence += 1;
          const turnId = asTurnId(`turn-many-settled-${sequence}`);
          liveFallback.codex.emit({
            type: "turn.completed",
            eventId: asEventId(`evt-many-settled-${sequence}`),
            provider: "codex",
            createdAt: new Date().toISOString(),
            threadId: input.threadId,
            turnId,
            payload: { state: "cancelled" },
          });
          yield* sleep(50);
          return { threadId: input.threadId, turnId };
        }),
      );
      yield* liveFallback.codex.waitForRuntimeSubscribers();

      yield* Effect.all(
        Array.from({ length: 12 }, (_, index) =>
          provider.sendTurn({ threadId, input: `turn ${index}` }),
        ),
        { concurrency: "unbounded" },
      );

      const binding = Option.getOrUndefined(yield* directory.getBinding(threadId));
      assert.equal(binding?.status, "stopped");
      const payload = binding?.runtimePayload as Record<string, unknown> | undefined;
      assert.notEqual(payload?.activeTurnId, asTurnId("turn-many-settled-1"));
    }),
  );
});
