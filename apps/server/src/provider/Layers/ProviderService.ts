/**
 * ProviderServiceLive - Cross-provider orchestration layer.
 *
 * Routes validated transport/API calls to provider adapters through
 * `ProviderAdapterRegistry` and `ProviderSessionDirectory`, and exposes a
 * unified provider event stream for subscribers.
 *
 * It does not implement provider protocol details (adapter concern).
 *
 * @module ProviderServiceLive
 */
import { createHmac } from "node:crypto";
import { isDeepStrictEqual } from "node:util";

import {
  defaultInstanceIdForDriver,
  ProviderCompactThreadInput,
  ProviderForkThreadInput,
  ModelSelection,
  NonNegativeInt,
  ThreadId,
  ProviderInterruptTurnInput,
  ProviderRespondToRequestInput,
  ProviderRespondToUserInputInput,
  ProviderSendTurnInput,
  ProviderStartReviewInput,
  ProviderSteerTurnInput,
  ProviderSessionStartInput,
  ProviderStopSessionInput,
  ProviderStartOptions,
  TurnId,
  type ProviderInstanceId,
  ProviderKind,
  type ProviderRuntimeEvent,
  type ProviderSession,
} from "@synara/contracts";
import {
  mergeProviderStartOptions,
  providerStartOptionsFromInstance,
  type ResolvedProviderInstance,
  resolveModelSelectionInstanceId,
  resolveProviderInstance,
} from "@synara/shared/providerInstances";
import {
  Array as EffectArray,
  Cause,
  Effect,
  Exit,
  Layer,
  Option,
  PubSub,
  Schema,
  SchemaIssue,
  Scope,
  Stream,
} from "effect";
import * as Semaphore from "effect/Semaphore";

import { ProviderUnsupportedError, ProviderValidationError } from "../Errors.ts";
import { ServerSecretStore } from "../../auth/Services/ServerSecretStore.ts";
import { ProviderAdapterRegistry } from "../Services/ProviderAdapterRegistry.ts";
import { makeProviderLifecycleCoordinator } from "../providerLifecycleCoordinator.ts";
import { ProviderService, type ProviderServiceShape } from "../Services/ProviderService.ts";
import {
  ProviderSessionDirectory,
  type ProviderRuntimeBinding,
  type ProviderSessionDirectoryWriteError,
} from "../Services/ProviderSessionDirectory.ts";
import { type EventNdjsonLogger, makeEventNdjsonLogger } from "./EventNdjsonLogger.ts";
import { AnalyticsService } from "../../telemetry/Services/AnalyticsService.ts";
import { ServerSettingsService } from "../../serverSettings.ts";
import { ProviderRuntimeEventRepository } from "../../persistence/Services/ProviderRuntimeEvents.ts";
import {
  codexSharedContinuationGeneration,
  codexSharedContinuationIdentityIsSafeMigration,
  parseCodexSharedContinuationIdentity,
  prepareProviderContinuationIdentity,
  prepareProviderContinuationIdentityForExplicitResume,
  providerContinuationIdentity,
} from "../continuationIdentity.ts";
import {
  classifyTerminalTurnApplicability,
  isStartedTurnApplicable,
} from "../terminalTurnApplicability.ts";
import { carryProviderAttachmentPaths } from "../providerAttachmentPaths.ts";

export interface ProviderServiceLiveOptions {
  readonly canonicalEventLogPath?: string;
  readonly canonicalEventLogger?: EventNdjsonLogger;
  readonly runtimeIdleStopMs?: number;
  /** Test/embedding override for the lossless runtime-event fan-out budget. */
  readonly runtimeEventBufferCapacity?: number;
  /** Production journal hook. The event must be durable before this effect returns. */
  readonly persistRuntimeEvent?: (event: ProviderRuntimeEvent) => Effect.Effect<void>;
}

const DEFAULT_PROVIDER_RUNTIME_IDLE_STOP_MS = 10 * 60 * 1000;
export const PROVIDER_RUNTIME_EVENT_BUFFER_CAPACITY = 2_048;
const configuredProviderRuntimeIdleStopMs = process.env.SYNARA_PROVIDER_RUNTIME_IDLE_STOP_MS;
const PROVIDER_RUNTIME_IDLE_STOP_MS = Number.isFinite(Number(configuredProviderRuntimeIdleStopMs))
  ? Math.max(0, Number(configuredProviderRuntimeIdleStopMs))
  : DEFAULT_PROVIDER_RUNTIME_IDLE_STOP_MS;
const PROVIDER_OPTIONS_FINGERPRINT_HMAC_SECRET = "provider-options-fingerprint-hmac-key";

const ProviderRollbackConversationInput = Schema.Struct({
  threadId: ThreadId,
  numTurns: NonNegativeInt,
});

type StopRuntimeSession = NonNullable<ProviderServiceShape["stopRuntimeSession"]>;
type StopRuntimeSessionInput = Parameters<StopRuntimeSession>[0];
type StopRuntimeSessionEffect = ReturnType<StopRuntimeSession>;
type InteractionResponse =
  | { readonly kind: "approval"; readonly input: ProviderRespondToRequestInput }
  | { readonly kind: "userInput"; readonly input: ProviderRespondToUserInputInput };

function toValidationError(
  operation: string,
  issue: string,
  cause?: unknown,
): ProviderValidationError {
  return new ProviderValidationError({
    operation,
    issue,
    ...(cause !== undefined ? { cause } : {}),
  });
}

const decodeInputOrValidationError = <S extends Schema.Top>(input: {
  readonly operation: string;
  readonly schema: S;
  readonly payload: unknown;
}) =>
  Schema.decodeUnknownEffect(input.schema)(input.payload).pipe(
    Effect.mapError(
      (schemaError) =>
        new ProviderValidationError({
          operation: input.operation,
          issue: SchemaIssue.makeFormatterDefault()(schemaError.issue),
          cause: schemaError,
        }),
    ),
  );

function toRuntimeStatus(session: ProviderSession): "starting" | "running" | "stopped" | "error" {
  if (session.status === "connecting") return "starting";
  if (session.status === "closed") return "stopped";
  return session.status === "error" ? "error" : "running";
}

function toRuntimePayloadFromSession(
  session: ProviderSession,
  credentialsFingerprintKey: Uint8Array,
  extra?: {
    readonly modelSelection?: unknown;
    readonly providerOptions?: unknown;
    readonly providerInstanceId?: string;
    readonly lastRuntimeEvent?: string;
    readonly lastRuntimeEventAt?: string;
    /**
     * Launch paths own the persisted launch options: when they carry no
     * providerOptions, the previous binding's options must be cleared instead
     * of surviving the runtime-payload merge, or recovery keeps starting the
     * thread with a home/credentials override the user already removed.
     */
    readonly launchOptionsAuthoritative?: boolean;
  },
): Record<string, unknown> {
  const persistedProviderOptions =
    extra?.providerOptions !== undefined
      ? redactProviderOptionsForPersistence(extra.providerOptions)
      : undefined;
  const credentialsFingerprint =
    Schema.is(ProviderKind)(session.provider) &&
    Schema.is(ProviderStartOptions)(extra?.providerOptions)
      ? credentialsFingerprintForProvider(
          session.provider,
          extra.providerOptions,
          credentialsFingerprintKey,
        )
      : undefined;
  const providerInstanceId = session.providerInstanceId ?? extra?.providerInstanceId;
  const continuationIdentity =
    Schema.is(ProviderKind)(session.provider) &&
    (extra?.launchOptionsAuthoritative === true || extra?.providerOptions !== undefined)
      ? providerContinuationIdentity(
          session.provider,
          Schema.is(ProviderStartOptions)(extra?.providerOptions)
            ? extra.providerOptions
            : undefined,
        )
      : undefined;
  return {
    cwd: session.cwd ?? null,
    model: session.model ?? null,
    ...(providerInstanceId ? { providerInstanceId } : {}),
    activeTurnId: session.activeTurnId ?? null,
    lastError: session.lastError ?? null,
    ...(extra?.modelSelection !== undefined ? { modelSelection: extra.modelSelection } : {}),
    ...(persistedProviderOptions !== undefined
      ? { providerOptions: persistedProviderOptions }
      : extra?.launchOptionsAuthoritative
        ? { providerOptions: null }
        : {}),
    ...(credentialsFingerprint !== undefined
      ? { providerOptionsCredentialsFingerprint: credentialsFingerprint }
      : extra?.launchOptionsAuthoritative
        ? { providerOptionsCredentialsFingerprint: null }
        : {}),
    ...(continuationIdentity !== undefined
      ? { continuationIdentity }
      : extra?.launchOptionsAuthoritative
        ? { continuationIdentity: null }
        : {}),
    ...(extra?.launchOptionsAuthoritative ? { continuationResetRequested: null } : {}),
    ...(extra?.lastRuntimeEvent !== undefined ? { lastRuntimeEvent: extra.lastRuntimeEvent } : {}),
    ...(extra?.lastRuntimeEventAt !== undefined
      ? { lastRuntimeEventAt: extra.lastRuntimeEventAt }
      : {}),
    ...(extra?.lifecycleGeneration !== undefined
      ? { lifecycleGeneration: extra.lifecycleGeneration }
      : {}),
  };
}

function redactProviderOptionsForPersistence(value: unknown): unknown {
  if (!Schema.is(ProviderStartOptions)(value)) {
    return value;
  }
  return {
    ...value,
    ...(value.codex ? { codex: withoutRuntimeEnvironment(value.codex) } : {}),
    ...(value.claudeAgent ? { claudeAgent: withoutRuntimeEnvironment(value.claudeAgent) } : {}),
    ...(value.cursor ? { cursor: withoutRuntimeEnvironment(value.cursor) } : {}),
    ...(value.gemini ? { gemini: withoutRuntimeEnvironment(value.gemini) } : {}),
    ...(value.grok ? { grok: withoutRuntimeEnvironment(value.grok) } : {}),
    ...(value.opencode
      ? { opencode: withoutServerPassword(withoutRuntimeEnvironment(value.opencode)) }
      : {}),
    ...(value.kilo ? { kilo: withoutServerPassword(withoutRuntimeEnvironment(value.kilo)) } : {}),
    ...(value.pi ? { pi: withoutRuntimeEnvironment(value.pi) } : {}),
  } satisfies ProviderStartOptions;
}

function withoutRuntimeEnvironment<T extends { readonly environment?: unknown }>(
  value: T,
): Omit<T, "environment"> {
  const { environment: _environment, ...rest } = value;
  return rest;
}

function withoutServerPassword<T extends { readonly serverPassword?: string | undefined }>(
  value: T,
): Omit<T, "serverPassword"> {
  const { serverPassword: _serverPassword, ...rest } = value;
  return rest;
}

function readPersistedModelSelection(
  runtimePayload: ProviderRuntimeBinding["runtimePayload"],
): ModelSelection | undefined {
  const raw = runtimePayloadRecord(runtimePayload).modelSelection;
  return Schema.is(ModelSelection)(raw) ? raw : undefined;
}

function readPersistedProviderOptions(
  runtimePayload: ProviderRuntimeBinding["runtimePayload"],
): ProviderStartOptions | undefined {
  const raw = runtimePayloadRecord(runtimePayload).providerOptions;
  return Option.getOrUndefined(Schema.decodeUnknownOption(ProviderStartOptions)(raw));
}

function redactedProviderOptionsForComparison(
  value: ProviderStartOptions | undefined,
): ProviderStartOptions | undefined {
  if (!value) {
    return undefined;
  }
  const redacted = redactProviderOptionsForPersistence(value);
  return Schema.is(ProviderStartOptions)(redacted) ? redacted : undefined;
}

// Fingerprints the credential inputs that persistence strips (environment,
// server passwords) so resume decisions can notice account/credential changes
// without ever persisting the secrets themselves.
function credentialsFingerprintForProvider(
  provider: ProviderKind,
  options: ProviderStartOptions | undefined,
  key: Uint8Array,
): string | undefined {
  const providerOptions = options?.[provider];
  if (!providerOptions || typeof providerOptions !== "object") {
    return undefined;
  }
  const environment = "environment" in providerOptions ? providerOptions.environment : undefined;
  const serverPassword =
    "serverPassword" in providerOptions ? providerOptions.serverPassword : undefined;
  const environmentEntries =
    environment && typeof environment === "object" && !Array.isArray(environment)
      ? Object.entries(environment as Record<string, unknown>).toSorted(([left], [right]) =>
          left.localeCompare(right),
        )
      : [];
  const password = typeof serverPassword === "string" && serverPassword ? serverPassword : null;
  if (environmentEntries.length === 0 && password === null) {
    return undefined;
  }
  return createHmac("sha256", key)
    .update(JSON.stringify({ environment: environmentEntries, serverPassword: password }))
    .digest("hex");
}

function readPersistedCredentialsFingerprint(
  runtimePayload: ProviderRuntimeBinding["runtimePayload"],
): string | undefined {
  if (!runtimePayload || typeof runtimePayload !== "object" || Array.isArray(runtimePayload)) {
    return undefined;
  }
  const raw =
    "providerOptionsCredentialsFingerprint" in runtimePayload
      ? runtimePayload.providerOptionsCredentialsFingerprint
      : undefined;
  return typeof raw === "string" && raw.length > 0 ? raw : undefined;
}

function readPersistedContinuationIdentity(
  runtimePayload: ProviderRuntimeBinding["runtimePayload"],
): string | undefined {
  if (!runtimePayload || typeof runtimePayload !== "object" || Array.isArray(runtimePayload)) {
    return undefined;
  }
  const raw = "continuationIdentity" in runtimePayload ? runtimePayload.continuationIdentity : null;
  return typeof raw === "string" && raw.length > 0 ? raw : undefined;
}

function readPersistedContinuationResetRequested(
  runtimePayload: ProviderRuntimeBinding["runtimePayload"],
): boolean {
  return Boolean(
    runtimePayload &&
    typeof runtimePayload === "object" &&
    !Array.isArray(runtimePayload) &&
    "continuationResetRequested" in runtimePayload &&
    runtimePayload.continuationResetRequested === true,
  );
}

function providerStartOptionsEqualForProvider(
  provider: ProviderKind,
  credentialsFingerprintKey: Uint8Array,
  persisted: {
    readonly options: ProviderStartOptions | undefined;
    readonly credentialsFingerprint: string | undefined;
  },
  current: ProviderStartOptions | undefined,
): boolean {
  return (
    isDeepStrictEqual(
      redactedProviderOptionsForComparison(persisted.options)?.[provider],
      redactedProviderOptionsForComparison(current)?.[provider],
    ) &&
    persisted.credentialsFingerprint ===
      credentialsFingerprintForProvider(provider, current, credentialsFingerprintKey)
  );
}

function providerUsesProtectedNativeContinuation(provider: string): boolean {
  return provider === "codex" || provider === "claudeAgent";
}

function persistedLaunchMatchesExactly(input: {
  readonly binding: ProviderRuntimeBinding;
  readonly provider: ProviderKind;
  readonly providerInstanceId: ProviderInstanceId;
  readonly providerOptions: ProviderStartOptions | undefined;
  readonly credentialsFingerprintKey: Uint8Array;
}): boolean {
  return (
    input.binding.provider === input.provider &&
    providerInstanceIdFromBinding(input.binding) === input.providerInstanceId &&
    providerStartOptionsEqualForProvider(
      input.provider,
      input.credentialsFingerprintKey,
      {
        options: readPersistedProviderOptions(input.binding.runtimePayload),
        credentialsFingerprint: readPersistedCredentialsFingerprint(input.binding.runtimePayload),
      },
      input.providerOptions,
    )
  );
}

function persistedContinuationMatchesLaunch(input: {
  readonly binding: ProviderRuntimeBinding;
  readonly provider: ProviderKind;
  readonly providerInstanceId: ProviderInstanceId;
  readonly providerOptions: ProviderStartOptions | undefined;
  readonly credentialsFingerprintKey: Uint8Array;
  readonly currentIdentity: string | undefined;
}): boolean {
  if (input.binding.provider !== input.provider) {
    return false;
  }
  const persistedIdentity = readPersistedContinuationIdentity(input.binding.runtimePayload);
  if (persistedIdentity !== undefined) {
    if (persistedIdentity === input.currentIdentity) {
      return input.provider !== "claudeAgent" || persistedLaunchMatchesExactly(input);
    }
    if (
      input.provider === "codex" &&
      codexSharedContinuationIdentityIsSafeMigration({
        persistedIdentity,
        currentIdentity: input.currentIdentity,
      })
    ) {
      return true;
    }
    return false;
  }

  // Legacy bindings predate continuation identities. Only exact launch
  // equivalence is safe until one successful resume persists the new identity.
  // A shared Codex source can have been deleted and recreated at the same path,
  // so path/config equivalence alone must never adopt its new generation.
  if (
    input.provider === "codex" &&
    parseCodexSharedContinuationIdentity(input.currentIdentity) !== undefined
  ) {
    return false;
  }
  return persistedLaunchMatchesExactly(input);
}

function prepareContinuationIdentityForCompatibility(input: {
  readonly operation: string;
  readonly provider: ProviderKind;
  readonly providerOptions: ProviderStartOptions | undefined;
  readonly persistedIdentity: string | undefined;
  readonly explicitResume?: boolean;
}) {
  return Effect.try({
    try: () =>
      input.explicitResume
        ? prepareProviderContinuationIdentityForExplicitResume(
            input.provider,
            input.providerOptions,
          )
        : prepareProviderContinuationIdentity(
            input.provider,
            input.providerOptions,
            input.persistedIdentity,
          ),
    catch: (cause) =>
      toValidationError(
        input.operation,
        cause instanceof Error
          ? cause.message
          : "Provider continuation storage could not be prepared safely.",
        cause,
      ),
  });
}

function incompatibleContinuationMessage(input: {
  readonly threadId: ThreadId;
  readonly previousProvider: string;
  readonly previousInstanceId: string;
  readonly nextProvider: ProviderKind;
  readonly nextInstanceId: string;
}): string {
  return `Cannot continue thread '${input.threadId}' from provider instance '${input.previousInstanceId}' (${input.previousProvider}) with '${input.nextInstanceId}' (${input.nextProvider}) because their native session storage is incompatible. Start a new thread or restore the original provider home.`;
}

function readPersistedProviderInstanceId(
  runtimePayload: ProviderRuntimeBinding["runtimePayload"],
): string | undefined {
  if (!runtimePayload || typeof runtimePayload !== "object" || Array.isArray(runtimePayload)) {
    return undefined;
  }
  const raw =
    "providerInstanceId" in runtimePayload ? runtimePayload.providerInstanceId : undefined;
  return typeof raw === "string" && raw.trim().length > 0 ? raw.trim() : undefined;
}

// getBinding materializes the driver-default instance id onto legacy rows, and
// default-instance sessions persist the default id in their payload. Only a
// NON-default id (or an instance-routed model selection) proves an explicit
// instance binding; everything else must keep seeding launches with the
// binding's persisted provider options.
function bindingHasExplicitProviderInstance(input: {
  readonly binding: ProviderRuntimeBinding;
  readonly persistedPayloadProviderInstanceId: string | undefined;
  readonly persistedModelSelection: ModelSelection | undefined;
}): boolean {
  const defaultId = defaultInstanceIdForDriver(input.binding.provider);
  return (
    (input.binding.providerInstanceId !== undefined &&
      input.binding.providerInstanceId !== defaultId) ||
    (input.persistedPayloadProviderInstanceId !== undefined &&
      input.persistedPayloadProviderInstanceId !== defaultId) ||
    (input.persistedModelSelection !== undefined &&
      resolveModelSelectionInstanceId(input.persistedModelSelection) !== input.binding.provider)
  );
}

function providerInstanceIdFromBinding(binding: ProviderRuntimeBinding): string {
  const persistedModelSelection = readPersistedModelSelection(binding.runtimePayload);
  return (
    binding.providerInstanceId ??
    readPersistedProviderInstanceId(binding.runtimePayload) ??
    (persistedModelSelection
      ? resolveModelSelectionInstanceId(persistedModelSelection)
      : binding.provider)
  );
}

function readPersistedCwd(
  runtimePayload: ProviderRuntimeBinding["runtimePayload"],
): string | undefined {
  const rawCwd = runtimePayloadRecord(runtimePayload).cwd;
  if (typeof rawCwd !== "string") return undefined;
  const trimmed = rawCwd.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

function modelSelectionForInstance(
  modelSelection: ModelSelection | undefined,
  instance: ResolvedProviderInstance,
): ModelSelection | undefined {
  return modelSelectionForRoute(modelSelection, instance.instanceId);
}

function modelSelectionForRoute(
  modelSelection: ModelSelection | undefined,
  providerInstanceId: string,
): ModelSelection | undefined {
  if (modelSelection === undefined) {
    return undefined;
  }
  return {
    ...modelSelection,
    instanceId: providerInstanceId,
  } as ModelSelection;
}

function sessionMatchesProviderInstance(
  session: ProviderSession,
  providerInstanceId: string,
  persistedProviderInstanceId?: string | undefined,
): boolean {
  const sessionProviderInstanceId = session.providerInstanceId ?? persistedProviderInstanceId;
  return (
    sessionProviderInstanceId === providerInstanceId ||
    (sessionProviderInstanceId === undefined && providerInstanceId === session.provider)
  );
}

function validateModelSelectionMatchesRoute(input: {
  readonly operation: string;
  readonly modelSelection?: ModelSelection | undefined;
  readonly provider: ProviderKind;
  readonly providerInstanceId: string;
}): ProviderValidationError | undefined {
  const selection = input.modelSelection;
  if (!selection) {
    return undefined;
  }
  if (selection.instanceId !== undefined) {
    if (selection.instanceId !== input.providerInstanceId) {
      return toValidationError(
        input.operation,
        `Model selection instance '${selection.instanceId}' does not match routed provider instance '${input.providerInstanceId}'.`,
      );
    }
    return undefined;
  }
  return undefined;
}

function runtimePayloadRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
}

function runtimeActiveTurnId(value: unknown): string | undefined {
  const activeTurnId = runtimePayloadRecord(value).activeTurnId;
  return typeof activeTurnId === "string" ? activeTurnId : undefined;
}

function hasResumeCursor(value: unknown): boolean {
  return value !== null && value !== undefined;
}

function makeKeyedThreadLock() {
  const entries = new Map<ThreadId, { readonly semaphore: Semaphore.Semaphore; users: number }>();
  return <A, E, R>(threadId: ThreadId, effect: Effect.Effect<A, E, R>): Effect.Effect<A, E, R> => {
    let entry = entries.get(threadId);
    if (entry === undefined) {
      entry = { semaphore: Semaphore.makeUnsafe(1), users: 0 };
      entries.set(threadId, entry);
    }
    entry.users += 1;
    const acquiredEntry = entry;
    return acquiredEntry.semaphore.withPermits(1)(effect).pipe(
      Effect.ensuring(
        Effect.sync(() => {
          acquiredEntry.users -= 1;
          if (acquiredEntry.users === 0 && entries.get(threadId) === acquiredEntry) {
            entries.delete(threadId);
          }
        }),
      ),
    );
  };
}

function providerKindConstraint(provider: string): {
  readonly provider?: ProviderKind | undefined;
} {
  return Schema.is(ProviderKind)(provider) ? { provider } : {};
}

function runtimeStatusForEvent(
  event: ProviderRuntimeEvent,
  activeTurnId?: unknown,
): "running" | "stopped" | "error" {
  switch (event.type) {
    case "session.state.changed":
      if (event.payload.state === "stopped") return "stopped";
      return event.payload.state === "error" ? "error" : "running";
    case "thread.state.changed":
      if (event.payload.state === "error") return "error";
      if (event.payload.state === "archived" || event.payload.state === "closed") return "stopped";
      return event.payload.state === "compacted" &&
        event.turnId === undefined &&
        activeTurnId == null
        ? "stopped"
        : "running";
    case "session.exited":
    case "turn.completed":
    case "turn.aborted":
      // A completed turn can still carry a resume cursor, but it must not keep
      // the desktop app treating the provider process as active after restart.
      return "stopped";
    case "runtime.error":
      return "error";
    default:
      return "running";
  }
}

function shouldRefreshResumeCursorForEvent(event: ProviderRuntimeEvent): boolean {
  return (
    event.type === "thread.started" ||
    event.type === "model.rerouted" ||
    (event.type === "thread.state.changed" &&
      event.payload.state === "compacted" &&
      event.turnId === undefined) ||
    event.type === "turn.tasks.updated" ||
    event.type === "turn.completed" ||
    event.type === "turn.aborted"
  );
}

function runtimeLastErrorForEvent(event: ProviderRuntimeEvent): string | null | undefined {
  if (event.type === "runtime.error") return event.payload.message;
  if (event.type === "session.state.changed")
    return event.payload.state === "error" ? (event.payload.reason ?? "Session error") : null;
  if (event.type === "thread.state.changed")
    return event.payload.state === "error" ? "Thread error" : null;
  return event.type === "turn.started" ||
    event.type === "turn.completed" ||
    event.type === "turn.aborted" ||
    event.type === "session.exited"
    ? null
    : undefined;
}

const makeProviderService = (options?: ProviderServiceLiveOptions) =>
  Effect.gen(function* () {
    const analytics = yield* Effect.service(AnalyticsService);
    const canonicalEventLogger =
      options?.canonicalEventLogger ??
      (options?.canonicalEventLogPath !== undefined
        ? yield* makeEventNdjsonLogger(options.canonicalEventLogPath, {
            stream: "canonical",
          })
        : undefined);

    const registry = yield* ProviderAdapterRegistry;
    const directory = yield* ProviderSessionDirectory;
    const lifecycle = makeProviderLifecycleCoordinator();
    for (const binding of yield* directory.listBindings()) {
      if (binding.lifecycleGeneration !== undefined) {
        lifecycle.adoptCurrent(binding.threadId, binding.lifecycleGeneration);
      }
    }
    const serverSettings = yield* ServerSettingsService;
    const secretStore = yield* ServerSecretStore;
    const credentialsFingerprintKey = yield* secretStore.getOrCreateRandom(
      PROVIDER_OPTIONS_FINGERPRINT_HMAC_SECRET,
      32,
    );
    const runtimeEventBufferCapacity = Math.max(
      1,
      Math.floor(options?.runtimeEventBufferCapacity ?? PROVIDER_RUNTIME_EVENT_BUFFER_CAPACITY),
    );
    const runtimeEventPubSub = yield* PubSub.bounded<ProviderRuntimeEvent>(
      runtimeEventBufferCapacity,
    );
    const runtimeEventProducerScope = yield* Scope.make("sequential");
    const runtimeIdleTimers = new Map<ThreadId, ReturnType<typeof setTimeout>>();
    // Fired idle callbacks outlive their timer map entry, so use generations to
    // invalidate async stop work when new user work starts in that gap.
    const runtimeIdleGenerations = new Map<ThreadId, symbol>();
    const runtimeIdleStopsInFlight = new Map<ThreadId, Promise<void>>();
    const runtimeIdleStopMs = Math.max(
      0,
      options?.runtimeIdleStopMs ?? PROVIDER_RUNTIME_IDLE_STOP_MS,
    );
    let stopIdleRuntimeSession: ((threadId: ThreadId, generation: symbol) => void) | null = null;

    const getAdapterForInstance = (
      instance: ResolvedProviderInstance,
      options?: { readonly allowDisabled?: boolean },
    ) =>
      registry.getByInstance
        ? registry.getByInstance(instance.instanceId, options)
        : registry.getByProvider(instance.driver);

    const getAdapterForBinding = (binding: ProviderRuntimeBinding) => {
      const instanceId = providerInstanceIdFromBinding(binding);
      const provider = Schema.is(ProviderKind)(binding.provider) ? binding.provider : null;
      if (instanceId && registry.getByInstance) {
        return registry
          .getByInstance(instanceId as ProviderInstanceId)
          .pipe(
            Effect.flatMap((adapter) =>
              provider && adapter.provider !== provider
                ? registry.getByProvider(provider)
                : Effect.succeed(adapter),
            ),
          )
          .pipe(
            Effect.catch(() =>
              provider
                ? registry.getByProvider(provider)
                : Effect.fail(new ProviderUnsupportedError({ provider: binding.provider })),
            ),
          );
      }
      return provider
        ? registry.getByProvider(provider)
        : Effect.fail(new ProviderUnsupportedError({ provider: binding.provider }));
    };

    const sessionWithPersistedProviderInstance = (
      session: ProviderSession,
    ): Effect.Effect<ProviderSession> =>
      Effect.gen(function* () {
        if (session.providerInstanceId !== undefined) {
          return session;
        }
        const binding = Option.getOrUndefined(
          yield* directory
            .getBinding(session.threadId)
            .pipe(Effect.orElseSucceed(() => Option.none<ProviderRuntimeBinding>())),
        );
        const persistedInstanceId =
          binding?.provider === session.provider
            ? providerInstanceIdFromBinding(binding)
            : undefined;
        return {
          ...session,
          providerInstanceId: (persistedInstanceId ?? session.provider) as ProviderInstanceId,
        };
      });

    const invalidateRuntimeIdleGeneration = (threadId: ThreadId): symbol => {
      const generation = Symbol(String(threadId));
      runtimeIdleGenerations.set(threadId, generation);
      return generation;
    };

    const isRuntimeIdleGenerationCurrent = (threadId: ThreadId, generation: symbol): boolean =>
      runtimeIdleGenerations.get(threadId) === generation;

    const retireRuntimeIdleGeneration = (threadId: ThreadId, generation?: symbol): void => {
      if (generation === undefined || isRuntimeIdleGenerationCurrent(threadId, generation)) {
        runtimeIdleGenerations.delete(threadId);
      }
    };

    const clearRuntimeIdleTimer = (threadId: ThreadId) => {
      invalidateRuntimeIdleGeneration(threadId);
      const timer = runtimeIdleTimers.get(threadId);
      if (!timer) {
        return;
      }
      clearTimeout(timer);
      runtimeIdleTimers.delete(threadId);
    };

    const scheduleRuntimeIdleStop = (threadId: ThreadId) => {
      clearRuntimeIdleTimer(threadId);
      if (runtimeIdleStopMs <= 0) {
        retireRuntimeIdleGeneration(threadId);
        return;
      }

      const generation = invalidateRuntimeIdleGeneration(threadId);
      const timer = setTimeout(() => {
        runtimeIdleTimers.delete(threadId);
        stopIdleRuntimeSession?.(threadId, generation);
      }, runtimeIdleStopMs);
      timer.unref();
      runtimeIdleTimers.set(threadId, timer);
    };

    const waitForRuntimeIdleStop = (threadId: ThreadId): Effect.Effect<void> =>
      Effect.promise(() => runtimeIdleStopsInFlight.get(threadId) ?? Promise.resolve());

    const resolveLaunchProviderInstance = (input: {
      readonly operation: string;
      readonly provider?: ProviderSessionStartInput["provider"];
      readonly providerInstanceId?: string | undefined;
      readonly modelSelection?: ModelSelection | undefined;
      readonly providerOptions?: ProviderStartOptions | undefined;
      /**
       * "instance" (default) lets settings-derived instance options override the
       * caller's options (browser-supplied options must not beat the server).
       * "caller" preserves persisted legacy launch options during recovery/fork,
       * where the session's recorded options are the source of truth.
       */
      readonly providerOptionsPrecedence?: "instance" | "caller";
      /**
       * Disabled instances must not start new sessions, but stop/cleanup paths
       * still need to resolve them to tear down runtimes and bindings that were
       * created before the instance was disabled.
       */
      readonly allowDisabled?: boolean;
    }) =>
      Effect.gen(function* () {
        const explicitProvider = input.provider !== undefined;
        const provider = input.provider ?? "codex";
        const requestedInstanceId =
          input.providerInstanceId ??
          (input.modelSelection ? resolveModelSelectionInstanceId(input.modelSelection) : provider);
        const settings = yield* serverSettings.getSettings.pipe(
          Effect.mapError((cause) =>
            toValidationError(input.operation, "Failed to load provider instance settings.", cause),
          ),
        );
        const instance = resolveProviderInstance(settings, {
          instanceId: requestedInstanceId,
          ...providerKindConstraint(explicitProvider ? provider : ""),
        });
        if (!instance) {
          return yield* toValidationError(
            input.operation,
            `Unknown provider instance '${requestedInstanceId}'.`,
          );
        }
        if (explicitProvider && provider !== instance.driver) {
          return yield* toValidationError(
            input.operation,
            `Requested provider '${provider}' does not match provider instance '${instance.instanceId}' driver '${instance.driver}'.`,
          );
        }
        if (!instance.enabled && input.allowDisabled !== true) {
          return yield* toValidationError(
            input.operation,
            `Provider instance '${instance.instanceId}' is disabled.`,
          );
        }
        return {
          instance,
          modelSelection: modelSelectionForInstance(input.modelSelection, instance),
          providerOptions:
            input.providerOptionsPrecedence === "caller"
              ? mergeProviderStartOptions(
                  providerStartOptionsFromInstance(instance),
                  input.providerOptions,
                )
              : mergeProviderStartOptions(
                  input.providerOptions,
                  providerStartOptionsFromInstance(instance),
                ),
        } as const;
      });

    const runIdleSensitiveProviderWork = <A, E, R>(
      threadId: ThreadId,
      effect: Effect.Effect<A, E, R>,
      options?: { readonly scheduleIdleStopOnSuccess?: boolean },
    ): Effect.Effect<A, E, R> =>
      Effect.suspend(() => {
        const existingIdleStop = runtimeIdleStopsInFlight.get(threadId);
        const displacedIdleStop = existingIdleStop !== undefined || runtimeIdleTimers.has(threadId);
        const waitForExistingIdleStop =
          existingIdleStop !== undefined ? Effect.promise(() => existingIdleStop) : Effect.void;
        return waitForExistingIdleStop.pipe(
          Effect.tap(() => Effect.sync(() => clearRuntimeIdleTimer(threadId))),
          Effect.flatMap(() => waitForRuntimeIdleStop(threadId)),
          Effect.flatMap(() => effect),
          Effect.onExit((exit) =>
            Exit.isSuccess(exit)
              ? options?.scheduleIdleStopOnSuccess === true
                ? Effect.sync(() => scheduleRuntimeIdleStop(threadId))
                : Effect.void
              : displacedIdleStop
                ? Effect.sync(() => scheduleRuntimeIdleStop(threadId))
                : Effect.sync(() => retireRuntimeIdleGeneration(threadId)),
          ),
        );
      });

    const reconcileRuntimeIdleTimer = (event: ProviderRuntimeEvent) => {
      if (event.type === "turn.started" || event.type === "session.exited") {
        clearRuntimeIdleTimer(event.threadId);
        if (event.type === "session.exited") retireRuntimeIdleGeneration(event.threadId);
        return;
      }
      if (
        event.type === "session.started" ||
        event.type === "thread.started" ||
        event.type === "turn.completed" ||
        event.type === "turn.aborted" ||
        (event.type === "thread.state.changed" &&
          (event.payload.state === "compacted" ||
            event.payload.state === "archived" ||
            event.payload.state === "closed"))
      ) {
        scheduleRuntimeIdleStop(event.threadId);
      }
    };

    const publishRuntimeEvent = (event: ProviderRuntimeEvent): Effect.Effect<void> =>
      Effect.gen(function* () {
        if (canonicalEventLogger) {
          yield* canonicalEventLogger.write(event, null);
        }
        yield* PubSub.publish(runtimeEventPubSub, event);
      });

    const correlateRuntimeEvent = (
      event: ProviderRuntimeEvent,
    ): Effect.Effect<ProviderRuntimeEvent | null> =>
      Effect.gen(function* () {
        const binding = Option.getOrUndefined(
          yield* directory.getBinding(event.threadId).pipe(
            Effect.catch((error) =>
              Effect.logWarning("failed to read provider runtime binding for event correlation", {
                threadId: event.threadId,
                provider: event.provider,
                error,
              }).pipe(Effect.as(Option.none<ProviderRuntimeBinding>())),
            ),
          ),
        );
        if (!binding) {
          return event;
        }
        if (binding.provider !== event.provider) {
          yield* Effect.logWarning("dropping provider event from stale provider binding", {
            threadId: event.threadId,
            eventProvider: event.provider,
            bindingProvider: binding.provider,
            eventType: event.type,
          });
          return null;
        }
        const bindingInstanceId =
          binding.providerInstanceId ?? readPersistedProviderInstanceId(binding.runtimePayload);
        if (
          event.providerInstanceId !== undefined &&
          bindingInstanceId !== undefined &&
          event.providerInstanceId !== bindingInstanceId
        ) {
          yield* Effect.logWarning("dropping provider event from stale provider instance", {
            threadId: event.threadId,
            provider: event.provider,
            eventInstanceId: event.providerInstanceId,
            bindingInstanceId,
            eventType: event.type,
          });
          return null;
        }
        if (event.providerInstanceId === undefined && bindingInstanceId !== undefined) {
          if (event.provider === "codex" && bindingInstanceId !== event.provider) {
            yield* Effect.logWarning("dropping untagged Codex event for non-default binding", {
              threadId: event.threadId,
              bindingInstanceId,
              eventType: event.type,
            });
            return null;
          }
          return { ...event, providerInstanceId: bindingInstanceId };
        }
        return event;
      });

    const upsertSessionBinding = (
      session: ProviderSession,
      threadId: ThreadId,
      extra?: {
        readonly lifecycleGeneration?: string;
        readonly modelSelection?: unknown;
        readonly providerOptions?: unknown;
        readonly providerInstanceId?: string;
        readonly lastRuntimeEvent?: string;
        readonly lastRuntimeEventAt?: string;
        readonly launchOptionsAuthoritative?: boolean;
      },
    ) =>
      directory.upsert({
        threadId,
        provider: session.provider,
        providerInstanceId:
          session.providerInstanceId ?? extra?.providerInstanceId ?? session.provider,
        runtimeMode: session.runtimeMode,
        status: toRuntimeStatus(session),
        ...(extra?.lifecycleGeneration !== undefined
          ? { lifecycleGeneration: extra.lifecycleGeneration }
          : {}),
        ...(session.resumeCursor !== undefined ? { resumeCursor: session.resumeCursor } : {}),
        runtimePayload: toRuntimePayloadFromSession(session, credentialsFingerprintKey, extra),
      });

    const upsertStoppedSessionBinding = (
      session: ProviderSession,
      stoppedAt: string,
    ): Effect.Effect<void, ProviderSessionDirectoryWriteError> =>
      directory.upsert({
        threadId: session.threadId,
        provider: session.provider,
        providerInstanceId: session.providerInstanceId ?? session.provider,
        runtimeMode: session.runtimeMode,
        status: "stopped",
        ...(session.resumeCursor !== undefined ? { resumeCursor: session.resumeCursor } : {}),
        runtimePayload: {
          ...toRuntimePayloadFromSession(session, credentialsFingerprintKey, {
            lastRuntimeEvent: "provider.stopAll",
            lastRuntimeEventAt: stoppedAt,
          }),
          activeTurnId: null,
        },
      });

    const markPersistedThreadStopped = (
      threadId: ThreadId,
      stoppedAt: string,
      session?: ProviderSession,
    ): Effect.Effect<void, ProviderSessionDirectoryWriteError> =>
      directory.getBinding(threadId).pipe(
        Effect.flatMap((bindingOption) =>
          Option.match(bindingOption, {
            onNone: () =>
              Effect.fail(
                new ProviderValidationError({
                  operation: "ProviderService.markPersistedThreadStopped",
                  issue: `No persisted provider binding found for thread '${threadId}'.`,
                }),
              ),
            onSome: (binding) =>
              directory.upsert({
                threadId,
                provider: binding.provider,
                providerInstanceId: binding.providerInstanceId,
                ...(binding.adapterKey !== undefined ? { adapterKey: binding.adapterKey } : {}),
                ...(binding.runtimeMode !== undefined ? { runtimeMode: binding.runtimeMode } : {}),
                status: "stopped",
                runtimePayload: {
                  activeTurnId: null,
                  lastRuntimeEvent: "provider.stopAll",
                  lastRuntimeEventAt: stoppedAt,
                },
              }),
          }),
        ),
      );

    // Runtime events are where adapters surface provider-native ids; refresh
    // from the live session before idle stop/recovery freezes an old cursor.
    const refreshResumeCursorFromActiveSession = (
      event: ProviderRuntimeEvent,
      binding: ProviderRuntimeBinding,
    ): Effect.Effect<unknown | null | undefined> => {
      if (!shouldRefreshResumeCursorForEvent(event)) {
        return Effect.succeed(binding.resumeCursor);
      }

      return Effect.gen(function* () {
        const adapter = yield* getAdapterForBinding(binding);
        const sessions = yield* adapter.listSessions();
        const activeSession = sessions.find(
          (session) =>
            session.threadId === event.threadId &&
            session.providerInstanceId === binding.providerInstanceId,
        );
        return activeSession?.resumeCursor ?? binding.resumeCursor;
      }).pipe(
        Effect.catchCause((cause) =>
          Effect.logWarning("provider.session.resume_cursor_refresh_failed", {
            threadId: event.threadId,
            provider: binding.provider,
            eventType: event.type,
            cause: Cause.pretty(cause),
          }).pipe(Effect.as(binding.resumeCursor)),
        ),
      );
    };

    // Turn ids whose terminal runtime event has already been observed, keyed by
    // thread. sendTurn consults this immediately before its post-dispatch
    // "running" upsert: a turn that settles before that write lands (e.g. a
    // pre-start cancellation) must not be re-marked as running afterwards.
    // A single slot per thread is not enough — sendTurn is not serialized per
    // thread, so overlapping sends can both settle pre-write and the second
    // completion would evict the first turn's marker before its send checked
    // it. Markers are retained only while dispatches are in flight, and each
    // sendTurn consumes its own marker.
    const recentlyCompletedTurnsByThread = new Map<ThreadId, Set<string>>();
    const recordRecentlyCompletedTurn = (threadId: ThreadId, turnId: string): void => {
      let turns = recentlyCompletedTurnsByThread.get(threadId);
      if (turns === undefined) {
        turns = new Set();
        recentlyCompletedTurnsByThread.set(threadId, turns);
      }
      turns.delete(turnId);
      turns.add(turnId);
    };
    const consumeRecentlyCompletedTurn = (threadId: ThreadId, turnId: string): boolean => {
      const turns = recentlyCompletedTurnsByThread.get(threadId);
      if (turns === undefined || !turns.has(turnId)) {
        return false;
      }
      turns.delete(turnId);
      if (turns.size === 0) {
        recentlyCompletedTurnsByThread.delete(threadId);
      }
      return true;
    };

    // Serializes binding writes for a thread between the runtime-event handler
    // and sendTurn's post-dispatch write. Without it a terminal event could
    // land between sendTurn's settled-turn check and its "running" upsert and
    // still be overwritten. Lifecycle events are low-frequency, so a per-thread
    // mutex adds no meaningful contention. Creation is synchronous
    // (Semaphore.makeUnsafe), so concurrent callers cannot mint two locks.
    const withBindingWriteLock = makeKeyedThreadLock();

    interface StartedTurnPersistenceInput {
      readonly threadId: ThreadId;
      readonly provider: ProviderRuntimeBinding["provider"];
      readonly providerInstanceId: ProviderInstanceId;
      readonly turnId: string;
      readonly generation: number;
      readonly resumeCursor?: unknown;
      readonly modelSelection?: unknown;
      readonly lastRuntimeEvent: string;
    }
    interface ThreadDispatchState {
      nextGeneration: number;
      latestGeneration: number;
      ownerGeneration: number;
      readonly inFlightGenerations: Set<number>;
      readonly outstandingTurnIds: Set<string>;
      readonly successfulResults: Map<number, StartedTurnPersistenceInput>;
    }
    const dispatchStateByThread = new Map<ThreadId, ThreadDispatchState>();
    const getDispatchState = (threadId: ThreadId): ThreadDispatchState => {
      let state = dispatchStateByThread.get(threadId);
      if (!state) {
        state = {
          nextGeneration: 0,
          latestGeneration: 0,
          ownerGeneration: 0,
          inFlightGenerations: new Set(),
          outstandingTurnIds: new Set(),
          successfulResults: new Map(),
        };
        dispatchStateByThread.set(threadId, state);
      }
      return state;
    };
    const beginTurnDispatch = (threadId: ThreadId): number => {
      const state = getDispatchState(threadId);
      const generation = state.nextGeneration + 1;
      state.nextGeneration = generation;
      state.latestGeneration = generation;
      state.inFlightGenerations.add(generation);
      return generation;
    };
    const cleanupDispatchState = (threadId: ThreadId): void => {
      const state = dispatchStateByThread.get(threadId);
      if (
        state &&
        state.inFlightGenerations.size === 0 &&
        state.outstandingTurnIds.size === 0 &&
        state.successfulResults.size === 0
      ) {
        dispatchStateByThread.delete(threadId);
      }
    };
    const rememberSuccessfulTurnDispatch = (input: StartedTurnPersistenceInput): void => {
      const state = getDispatchState(input.threadId);
      state.outstandingTurnIds.add(input.turnId);
      state.successfulResults.set(input.generation, input);
    };
    const hasAmbiguousTerminalTurn = (threadId: ThreadId): boolean => {
      const state = dispatchStateByThread.get(threadId);
      return (
        state !== undefined &&
        (state.outstandingTurnIds.size > 1 ||
          state.inFlightGenerations.size > 1 ||
          (state.outstandingTurnIds.size > 0 && state.inFlightGenerations.size > 0))
      );
    };

    const persistStartedTurn = (input: StartedTurnPersistenceInput) => {
      let persistenceAttempted = false;
      const rollbackFailedPersistence = Effect.sync(() => {
        if (!persistenceAttempted) return;
        const state = dispatchStateByThread.get(input.threadId);
        state?.successfulResults.delete(input.generation);
        state?.outstandingTurnIds.delete(input.turnId);
        cleanupDispatchState(input.threadId);
      });
      const markPersistenceSucceeded = (ownsLifecycle: boolean): void => {
        const state = getDispatchState(input.threadId);
        if (ownsLifecycle) state.ownerGeneration = input.generation;
        for (const generation of state.successfulResults.keys()) {
          if (generation <= input.generation) state.successfulResults.delete(generation);
        }
      };

      return withBindingWriteLock(
        input.threadId,
        Effect.gen(function* () {
          // Older successful results stay retained while newer invocations are
          // unresolved. If every newer generation fails, settlement promotes
          // the newest retained result through this same persistence path.
          if (getDispatchState(input.threadId).latestGeneration !== input.generation) {
            return;
          }
          const completedBeforePersistence = consumeRecentlyCompletedTurn(
            input.threadId,
            input.turnId,
          );
          if (completedBeforePersistence) {
            getDispatchState(input.threadId).outstandingTurnIds.delete(input.turnId);
          }
          persistenceAttempted = true;
          if (completedBeforePersistence) {
            // An existing row may already belong to a newer overlapping turn;
            // the delayed result must not overwrite any of its metadata. With
            // no row, preserve the live-fallback behavior by creating an
            // explicitly stopped binding from the settled dispatch result.
            if (Option.isSome(yield* directory.getBinding(input.threadId))) {
              markPersistenceSucceeded(false);
              return;
            }
            yield* directory.upsert({
              threadId: input.threadId,
              provider: input.provider,
              providerInstanceId: input.providerInstanceId,
              status: "stopped",
              ...(input.resumeCursor !== undefined ? { resumeCursor: input.resumeCursor } : {}),
              ...(input.modelSelection !== undefined
                ? { runtimePayload: { modelSelection: input.modelSelection } }
                : {}),
            });
            markPersistenceSucceeded(false);
            return;
          }

          // Clear again under the binding lock. This orders active-turn writes
          // against terminal-event scheduling even if dispatch took long
          // enough for an older terminal event to arrive in the meantime.
          clearRuntimeIdleTimer(input.threadId);
          yield* directory.upsert({
            threadId: input.threadId,
            provider: input.provider,
            providerInstanceId: input.providerInstanceId,
            status: "running",
            ...(input.resumeCursor !== undefined ? { resumeCursor: input.resumeCursor } : {}),
            runtimePayload: {
              ...(input.modelSelection !== undefined
                ? { modelSelection: input.modelSelection }
                : {}),
              activeTurnId: input.turnId,
              lastRuntimeEvent: input.lastRuntimeEvent,
              lastRuntimeEventAt: new Date().toISOString(),
            },
          });
          markPersistenceSucceeded(true);
        }),
      ).pipe(Effect.onError(() => rollbackFailedPersistence));
    };

    const finishTurnDispatch = (
      threadId: ThreadId,
      generation: number,
    ): Effect.Effect<void, ProviderSessionDirectoryWriteError> =>
      Effect.gen(function* () {
        const candidate = yield* Effect.sync(() => {
          const state = getDispatchState(threadId);
          state.inFlightGenerations.delete(generation);
          if (state.latestGeneration === generation && !state.successfulResults.has(generation)) {
            state.latestGeneration = Math.max(
              state.ownerGeneration,
              ...state.inFlightGenerations,
              ...state.successfulResults.keys(),
            );
          }
          return state.successfulResults.get(state.latestGeneration);
        });
        if (candidate !== undefined) {
          yield* persistStartedTurn(candidate);
        }
      }).pipe(
        Effect.ensuring(
          Effect.sync(() => {
            const state = dispatchStateByThread.get(threadId);
            if (state?.inFlightGenerations.size === 0) {
              recentlyCompletedTurnsByThread.delete(threadId);
            }
            cleanupDispatchState(threadId);
          }),
        ),
      );

    const runTurnDispatch = <A, E, R>(
      threadId: ThreadId,
      dispatch: (generation: number) => Effect.Effect<A, E, R>,
    ) =>
      runIdleSensitiveProviderWork(
        threadId,
        Effect.suspend(() => {
          const generation = beginTurnDispatch(threadId);
          return dispatch(generation).pipe(
            Effect.ensuring(finishTurnDispatch(threadId, generation).pipe(Effect.ignore)),
          );
        }),
      );

    const updateSessionBindingFromRuntimeEvent = (
      event: ProviderRuntimeEvent,
    ): Effect.Effect<void> => {
      switch (event.type) {
        case "session.started":
        case "session.state.changed":
        case "thread.started":
        case "thread.state.changed":
        case "turn.started":
        case "turn.tasks.updated":
        case "model.rerouted":
        case "turn.completed":
        case "turn.aborted":
        case "session.exited":
        case "runtime.error":
          break;
        default:
          return Effect.void;
      }

      return withBindingWriteLock(
        event.threadId,
        Effect.gen(function* () {
          if (event.type === "turn.started" && event.turnId !== undefined) {
            getDispatchState(event.threadId).outstandingTurnIds.add(String(event.turnId));
          }
          if (
            (event.type === "turn.completed" || event.type === "turn.aborted") &&
            event.turnId !== undefined &&
            (dispatchStateByThread.get(event.threadId)?.inFlightGenerations.size ?? 0) > 0
          ) {
            recordRecentlyCompletedTurn(event.threadId, String(event.turnId));
          }
          const binding = Option.getOrUndefined(yield* directory.getBinding(event.threadId));
          if (!binding) {
            reconcileRuntimeIdleTimer(event);
            return;
          }
          if (binding.provider !== event.provider) {
            return;
          }
          if (
            event.lifecycleGeneration !== undefined &&
            binding.lifecycleGeneration !== event.lifecycleGeneration
          ) {
            return;
          }

          const currentActiveTurnId = runtimeActiveTurnId(binding.runtimePayload);
          if (
            event.type === "turn.started" &&
            !isStartedTurnApplicable({
              activeTurnId: currentActiveTurnId,
              eventTurnId: event.turnId === undefined ? undefined : String(event.turnId),
            })
          ) {
            return;
          }
          if (event.type === "turn.completed" || event.type === "turn.aborted") {
            const applicability = classifyTerminalTurnApplicability({
              activeTurnId: currentActiveTurnId,
              eventTurnId: event.turnId === undefined ? undefined : String(event.turnId),
              hasAmbiguousTurns: hasAmbiguousTerminalTurn(event.threadId),
            });
            if (!applicability.applicable) {
              if (event.turnId !== undefined) {
                dispatchStateByThread
                  .get(event.threadId)
                  ?.outstandingTurnIds.delete(String(event.turnId));
                cleanupDispatchState(event.threadId);
              }
              if (applicability.reason === "ambiguous-missing-turn-id") {
                yield* Effect.logWarning("provider.session.ambiguous_terminal_event_ignored", {
                  threadId: event.threadId,
                  eventType: event.type,
                });
              }
              return;
            }
            if (event.turnId === undefined && applicability.resolvedTurnId !== undefined) {
              recordRecentlyCompletedTurn(event.threadId, applicability.resolvedTurnId);
            }
            if (applicability.resolvedTurnId !== undefined) {
              dispatchStateByThread
                .get(event.threadId)
                ?.outstandingTurnIds.delete(applicability.resolvedTurnId);
              cleanupDispatchState(event.threadId);
            }
          }
          const activeTurnId =
            event.type === "turn.started"
              ? (event.turnId ?? null)
              : event.type === "thread.state.changed" && event.payload.state === "compacted"
                ? (event.turnId ?? currentActiveTurnId)
                : event.type === "turn.completed" ||
                    event.type === "turn.aborted" ||
                    (event.type === "thread.state.changed" &&
                      (event.payload.state === "archived" ||
                        event.payload.state === "closed" ||
                        event.payload.state === "error")) ||
                    event.type === "session.exited" ||
                    event.type === "runtime.error" ||
                    (event.type === "session.state.changed" &&
                      (event.payload.state === "ready" ||
                        event.payload.state === "stopped" ||
                        event.payload.state === "error"))
                  ? null
                  : currentActiveTurnId;
          const lastError = runtimeLastErrorForEvent(event);
          const resumeCursor = yield* refreshResumeCursorFromActiveSession(event, binding);

          yield* directory.upsert({
            threadId: event.threadId,
            provider: binding.provider,
            providerInstanceId: binding.providerInstanceId,
            ...(binding.adapterKey !== undefined ? { adapterKey: binding.adapterKey } : {}),
            ...(binding.runtimeMode !== undefined ? { runtimeMode: binding.runtimeMode } : {}),
            status: runtimeStatusForEvent(event, activeTurnId),
            ...(resumeCursor !== undefined ? { resumeCursor } : {}),
            runtimePayload: {
              activeTurnId,
              lastRuntimeEvent: event.type,
              lastRuntimeEventAt: event.createdAt,
              ...(lastError !== undefined ? { lastError } : {}),
            },
          });
          if (event.type === "session.exited") {
            const dispatchState = dispatchStateByThread.get(event.threadId);
            if (dispatchState) {
              // Invalidate adapter calls that were already in flight when the
              // session exited, then retain only the generations needed for
              // their eventual settlement/cleanup.
              dispatchState.latestGeneration = dispatchState.nextGeneration + 1;
              dispatchState.nextGeneration = dispatchState.latestGeneration;
              dispatchState.outstandingTurnIds.clear();
              dispatchState.successfulResults.clear();
            }
            recentlyCompletedTurnsByThread.delete(event.threadId);
            cleanupDispatchState(event.threadId);
          }
          reconcileRuntimeIdleTimer(event);
        }),
      ).pipe(
        Effect.catchCause((cause) =>
          Effect.logWarning("provider.session.runtime_binding_update_failed", {
            threadId: event.threadId,
            eventType: event.type,
            cause: Cause.pretty(cause),
          }),
        ),
      );
    };

    const providers = yield* registry.listProviders();
    const adapters = yield* Effect.forEach(providers, (provider) =>
      registry.getByProvider(provider),
    );
    const processRuntimeEvent = (event: ProviderRuntimeEvent): Effect.Effect<void> =>
      Effect.uninterruptible(
        Effect.gen(function* () {
        if (
          event.lifecycleGeneration !== undefined &&
          lifecycle.currentGeneration(event.threadId) !== event.lifecycleGeneration
        ) {
          yield* Effect.logDebug("provider.session.stale_generation_event_ignored", {
            threadId: event.threadId,
            provider: event.provider,
            eventType: event.type,
            eventLifecycleGeneration: event.lifecycleGeneration,
          });
          return;
        }
        const correlatedEvent = yield* correlateRuntimeEvent(event);
        if (correlatedEvent === null) {
          return;
        }
        if (correlatedEvent.type === "turn.started") {
          reconcileRuntimeIdleTimer(correlatedEvent);
        }
        yield* updateSessionBindingFromRuntimeEvent(correlatedEvent);
        if (correlatedEvent.type !== "turn.started") {
          reconcileRuntimeIdleTimer(correlatedEvent);
        }
        yield* publishRuntimeEvent(correlatedEvent);
        }),
      );

    // Fan provider events straight into the bounded pubsub so high-volume
    // streams backpressure at one lossless owner without an extra queue hop.
    yield* Effect.forEach(adapters, (adapter) =>
      Stream.runForEach(adapter.streamEvents, processRuntimeEvent).pipe(
        Effect.forkIn(runtimeEventProducerScope),
      ),
    ).pipe(Effect.asVoid);

    const recoverSessionForThread = (input: {
      readonly binding: ProviderRuntimeBinding;
      readonly operation: string;
    }) =>
      lifecycle.run(input.binding.threadId, (lease) =>
      Effect.gen(function* () {
        const binding = Option.getOrUndefined(
          yield* directory.getBinding(input.binding.threadId),
        );
        if (!binding) {
          return yield* toValidationError(
            input.operation,
            `Cannot recover thread '${input.binding.threadId}' because its provider binding was removed.`,
          );
        }
        const hasPersistedResumeCursor = hasResumeCursor(binding.resumeCursor);
        const persistedModelSelection = readPersistedModelSelection(binding.runtimePayload);
        const persistedProviderOptions = readPersistedProviderOptions(binding.runtimePayload);
        const persistedPayloadProviderInstanceId = readPersistedProviderInstanceId(
          binding.runtimePayload,
        );
        const persistedProviderInstanceId = providerInstanceIdFromBinding(binding);
        const hasProviderInstanceBinding = bindingHasExplicitProviderInstance({
          binding,
          persistedPayloadProviderInstanceId,
          persistedModelSelection,
        });
        const resolved = yield* resolveLaunchProviderInstance({
          operation: input.operation,
          ...providerKindConstraint(binding.provider),
          providerInstanceId: persistedProviderInstanceId,
          ...(persistedModelSelection ? { modelSelection: persistedModelSelection } : {}),
          ...(!hasProviderInstanceBinding && persistedProviderOptions
            ? {
                providerOptions: persistedProviderOptions,
                providerOptionsPrecedence: "caller" as const,
              }
            : {}),
        });
        const currentContinuationIdentity =
          hasPersistedResumeCursor && binding.provider === resolved.instance.driver
            ? yield* prepareContinuationIdentityForCompatibility({
                operation: input.operation,
                provider: resolved.instance.driver,
                providerOptions: resolved.providerOptions,
                persistedIdentity: readPersistedContinuationIdentity(binding.runtimePayload),
              })
            : undefined;
        const canReusePersistedResumeCursor =
          hasPersistedResumeCursor &&
          persistedContinuationMatchesLaunch({
            binding,
            provider: resolved.instance.driver,
            providerInstanceId: resolved.instance.instanceId,
            providerOptions: resolved.providerOptions,
            credentialsFingerprintKey,
            currentIdentity: currentContinuationIdentity,
          });
        const expectedCodexContinuationGeneration = canReusePersistedResumeCursor
          ? codexSharedContinuationGeneration(currentContinuationIdentity)
          : undefined;
        if (
          canReusePersistedResumeCursor &&
          resolved.instance.driver === "codex" &&
          expectedCodexContinuationGeneration === undefined
        ) {
          return yield* toValidationError(
            input.operation,
            "Cannot recover a Codex native thread because the persisted continuation source has no verified generation.",
          );
        }
        if (
          hasPersistedResumeCursor &&
          providerUsesProtectedNativeContinuation(resolved.instance.driver) &&
          !canReusePersistedResumeCursor
        ) {
          return yield* toValidationError(
            input.operation,
            incompatibleContinuationMessage({
              threadId: binding.threadId,
              previousProvider: binding.provider,
              previousInstanceId: providerInstanceIdFromBinding(binding),
              nextProvider: resolved.instance.driver,
              nextInstanceId: resolved.instance.instanceId,
            }),
          );
        }
        const adapter = yield* getAdapterForInstance(resolved.instance);
        const providerAdapter = yield* registry.getByProvider(resolved.instance.driver);
        const activeSessions = yield* providerAdapter.listSessions();
        const existing = activeSessions.find(
          (session) => session.threadId === binding.threadId,
        );
        if (existing) {
          if (
            sessionMatchesProviderInstance(
              existing,
              resolved.instance.instanceId,
              persistedProviderInstanceId,
            )
          ) {
            const existingWithInstance: ProviderSession = {
              ...existing,
              providerInstanceId: existing.providerInstanceId ?? resolved.instance.instanceId,
            };
            lease.adopt(binding.lifecycleGeneration ?? "legacy");
            yield* upsertSessionBinding(existingWithInstance, binding.threadId, {
              lifecycleGeneration: binding.lifecycleGeneration ?? "legacy",
            });
            yield* analytics.record("provider.session.recovered", {
              provider: existing.provider,
              providerInstanceId: resolved.instance.instanceId,
              strategy: "adopt-existing",
              hasResumeCursor: hasResumeCursor(existing.resumeCursor),
            });
            return { adapter, session: existingWithInstance } as const;
          }

          yield* providerAdapter.stopSession(binding.threadId).pipe(
            Effect.tap(() =>
              analytics.record("provider.session.stopped", {
                provider: providerAdapter.provider,
                providerInstanceId: existing.providerInstanceId,
                reason: "stale-provider-instance",
              }),
            ),
            Effect.catchCause((cause) =>
              Effect.logWarning("provider.session.stop-stale-failed", {
                threadId: binding.threadId,
                provider: providerAdapter.provider,
                providerInstanceId: existing.providerInstanceId,
                cause: Cause.pretty(cause),
              }),
            ),
          );
        }

        const persistedCwd = readPersistedCwd(binding.runtimePayload);

        const resumed = yield* adapter.startSession({
          threadId: binding.threadId,
          provider: resolved.instance.driver,
          providerInstanceId: resolved.instance.instanceId,
          lifecycleGeneration: lease.generation,
          ...(persistedCwd ? { cwd: persistedCwd } : {}),
          ...(resolved.modelSelection ? { modelSelection: resolved.modelSelection } : {}),
          ...(resolved.providerOptions ? { providerOptions: resolved.providerOptions } : {}),
          ...(canReusePersistedResumeCursor ? { resumeCursor: binding.resumeCursor } : {}),
          ...(expectedCodexContinuationGeneration ? { expectedCodexContinuationGeneration } : {}),
          runtimeMode: binding.runtimeMode ?? "full-access",
        });
        if (resumed.provider !== adapter.provider) {
          return yield* toValidationError(
            input.operation,
            `Adapter/provider mismatch while recovering thread '${binding.threadId}'. Expected '${adapter.provider}', received '${resumed.provider}'.`,
          );
        }

        const resumedWithInstance: ProviderSession = {
          ...resumed,
          providerInstanceId: resolved.instance.instanceId,
        };
        yield* upsertSessionBinding(resumedWithInstance, binding.threadId, {
          lifecycleGeneration: lease.generation,
          ...(resolved.modelSelection ? { modelSelection: resolved.modelSelection } : {}),
          ...(resolved.providerOptions ? { providerOptions: resolved.providerOptions } : {}),
          launchOptionsAuthoritative: true,
        });
        yield* analytics.record("provider.session.recovered", {
          provider: resumed.provider,
          providerInstanceId: resolved.instance.instanceId,
          strategy: "resume-thread",
          hasResumeCursor: hasResumeCursor(resumed.resumeCursor),
        });
        return { adapter, session: resumedWithInstance } as const;
      }),
      );

    const findLiveSession = (threadId: ThreadId) =>
      Effect.gen(function* () {
        const matches = yield* Effect.forEach(
          adapters,
          (adapter) =>
            adapter.listSessions().pipe(
              Effect.map((sessions) => {
                const session = sessions.find((candidate) => candidate.threadId === threadId);
                return session ? { adapter, session } : null;
              }),
              Effect.orElseSucceed(() => null),
            ),
          { concurrency: "unbounded" },
        );
        const listedMatch = matches.find((match) => match !== null);
        if (listedMatch) {
          return listedMatch;
        }
        const adapterMatches = yield* Effect.forEach(
          adapters,
          (adapter) =>
            adapter.hasSession(threadId).pipe(
              Effect.map((hasSession) =>
                hasSession ? { adapter, session: null as ProviderSession | null } : null,
              ),
              Effect.orElseSucceed(() => null),
            ),
          { concurrency: "unbounded" },
        );
        return adapterMatches.find((match) => match !== null) ?? null;
      });

    const stopStaleSessionsForThread = (input: {
      readonly threadId: ThreadId;
      readonly provider: ProviderKind;
      readonly providerInstanceId: string;
    }): Effect.Effect<void> =>
      Effect.forEach(
        adapters,
        (adapter) =>
          Effect.gen(function* () {
            const binding = Option.getOrUndefined(
              yield* directory
                .getBinding(input.threadId)
                .pipe(Effect.orElseSucceed(() => Option.none<ProviderRuntimeBinding>())),
            );
            const bindingProviderInstanceId =
              binding?.provider === adapter.provider
                ? providerInstanceIdFromBinding(binding)
                : undefined;
            const activeSessions = yield* adapter.listSessions();
            const staleSession = activeSessions.find((session) => {
              if (session.threadId !== input.threadId) {
                return false;
              }
              if (adapter.provider !== input.provider) {
                return true;
              }
              return !sessionMatchesProviderInstance(
                session,
                input.providerInstanceId,
                bindingProviderInstanceId,
              );
            });
            if (!staleSession) {
              return;
            }
            yield* adapter.stopSession(input.threadId).pipe(
              Effect.tap(() =>
                analytics.record("provider.session.stopped", {
                  provider: adapter.provider,
                  providerInstanceId: staleSession.providerInstanceId,
                  reason: "stale-provider-instance",
                }),
              ),
              Effect.catchCause((cause) =>
                Effect.logWarning("provider.session.stop-stale-failed", {
                  threadId: input.threadId,
                  provider: adapter.provider,
                  providerInstanceId: staleSession.providerInstanceId,
                  cause: Cause.pretty(cause),
                }),
              ),
            );
          }),
        { discard: true, concurrency: "unbounded" },
      ).pipe(Effect.asVoid);

    const providerInstanceExists = (input: {
      readonly operation: string;
      readonly instanceId: string;
      readonly provider: string;
    }) =>
      serverSettings.getSettings.pipe(
        Effect.mapError((cause) =>
          toValidationError(input.operation, "Failed to load provider instance settings.", cause),
        ),
        Effect.map((settings) =>
          Boolean(
            resolveProviderInstance(settings, {
              instanceId: input.instanceId,
              ...providerKindConstraint(input.provider),
            }),
          ),
        ),
      );

    const resolveRoutableSession = (input: {
      readonly threadId: ThreadId;
      readonly operation: string;
      readonly allowRecovery: boolean;
      /** Stop/cleanup paths must still route sessions of disabled instances. */
      readonly allowDisabled?: boolean;
      /**
       * Stop/cleanup paths must also tear down runtimes whose provider
       * instance was deleted from settings; those route by the persisted
       * binding (or live session) instead of failing instance resolution.
       */
      readonly allowDeleted?: boolean;
    }) =>
      Effect.gen(function* () {
        const binding = Option.getOrUndefined(yield* directory.getBinding(input.threadId));
        if (!binding) {
          // Startup extension prompts can fire before startSession has persisted
          // the provider binding, but the adapter already owns a live session.
          const live = yield* findLiveSession(input.threadId);
          if (live) {
            const liveProvider = live.session?.provider ?? live.adapter.provider;
            const liveProviderInstanceId = live.session?.providerInstanceId ?? liveProvider;
            if (input.allowDeleted === true) {
              const exists = yield* providerInstanceExists({
                operation: input.operation,
                instanceId: liveProviderInstanceId,
                provider: liveProvider,
              });
              if (!exists) {
              return {
                adapter: live.adapter,
                threadId: input.threadId,
                providerInstanceId: liveProviderInstanceId,
                isActive: true,
                lifecycleGeneration: lifecycle.currentGeneration(input.threadId),
              } as const;
              }
            }
            const resolved = yield* resolveLaunchProviderInstance({
              operation: input.operation,
              provider: liveProvider,
              providerInstanceId: liveProviderInstanceId,
              ...(input.allowDisabled !== undefined ? { allowDisabled: input.allowDisabled } : {}),
            });
            return {
              adapter: live.adapter,
              threadId: input.threadId,
              providerInstanceId: resolved.instance.instanceId,
              isActive: true,
              lifecycleGeneration: lifecycle.currentGeneration(input.threadId),
            } as const;
          }
          return yield* toValidationError(
            input.operation,
            `Cannot route thread '${input.threadId}' because no persisted provider binding exists.`,
          );
        }
        const bindingProviderInstanceId = providerInstanceIdFromBinding(binding);
        if (input.allowDeleted === true) {
          const exists = yield* providerInstanceExists({
            operation: input.operation,
            instanceId: bindingProviderInstanceId,
            provider: binding.provider,
          });
          if (!exists) {
            const adapter = yield* getAdapterForBinding(binding);
            const isActive = yield* adapter.hasSession(input.threadId);
            return {
              adapter,
              threadId: input.threadId,
              providerInstanceId: bindingProviderInstanceId,
              isActive,
              lifecycleGeneration: binding.lifecycleGeneration,
            } as const;
          }
        }
        const resolved = yield* resolveLaunchProviderInstance({
          operation: input.operation,
          ...providerKindConstraint(binding.provider),
          providerInstanceId: bindingProviderInstanceId,
          ...(input.allowDisabled !== undefined ? { allowDisabled: input.allowDisabled } : {}),
        });
        const adapter = yield* getAdapterForInstance(
          resolved.instance,
          input.allowDisabled !== undefined ? { allowDisabled: input.allowDisabled } : undefined,
        );
        const providerAdapter = yield* registry.getByProvider(resolved.instance.driver);
        const activeSessions = yield* providerAdapter.listSessions();
        const activeSession = activeSessions.find((session) => session.threadId === input.threadId);
        if (
          activeSession &&
          sessionMatchesProviderInstance(
            activeSession,
            resolved.instance.instanceId,
            bindingProviderInstanceId,
          )
        ) {
          return {
            adapter,
            threadId: input.threadId,
            providerInstanceId: resolved.instance.instanceId,
            isActive: true,
            lifecycleGeneration: binding.lifecycleGeneration,
          } as const;
        }
        if (activeSession) {
          yield* stopStaleSessionsForThread({
            threadId: input.threadId,
            provider: resolved.instance.driver,
            providerInstanceId: resolved.instance.instanceId,
          });
        }

        if (!input.allowRecovery) {
          return {
            adapter,
            threadId: input.threadId,
            providerInstanceId: resolved.instance.instanceId,
            isActive: false,
            lifecycleGeneration: binding.lifecycleGeneration,
          } as const;
        }

        const recovered = yield* recoverSessionForThread({ binding, operation: input.operation });
        return {
          adapter: recovered.adapter,
          threadId: input.threadId,
          providerInstanceId: recovered.session.providerInstanceId ?? recovered.adapter.provider,
          isActive: true,
          lifecycleGeneration: lifecycle.currentGeneration(input.threadId),
        } as const;
      });

    // Lets the command reactor seed its in-memory option cache from the durable
    // binding without exposing persisted credential fingerprints or secrets.
    const sessionBindingMatchesLaunchOptions: NonNullable<
      ProviderServiceShape["sessionBindingMatchesLaunchOptions"]
    > = (input) =>
      Effect.gen(function* () {
        const binding = Option.getOrUndefined(yield* directory.getBinding(input.threadId));
        if (
          !binding ||
          binding.provider !== input.provider ||
          providerInstanceIdFromBinding(binding) !== input.providerInstanceId
        ) {
          return false;
        }

        const resolved = yield* resolveLaunchProviderInstance({
          operation: "ProviderService.sessionBindingMatchesLaunchOptions",
          provider: input.provider,
          providerInstanceId: input.providerInstanceId,
          ...(input.providerOptions !== undefined
            ? { providerOptions: input.providerOptions }
            : {}),
        });
        return providerStartOptionsEqualForProvider(
          resolved.instance.driver,
          credentialsFingerprintKey,
          {
            options: readPersistedProviderOptions(binding.runtimePayload),
            credentialsFingerprint: readPersistedCredentialsFingerprint(binding.runtimePayload),
          },
          resolved.providerOptions,
        );
      });

    const startSession: ProviderServiceShape["startSession"] = (threadId, rawInput) =>
      Effect.gen(function* () {
        const parsed = yield* decodeInputOrValidationError({
          operation: "ProviderService.startSession",
          schema: ProviderSessionStartInput,
          payload: rawInput,
        });

        const input = {
          ...parsed,
          threadId,
          provider: parsed.provider ?? "codex",
        };
        clearRuntimeIdleTimer(threadId);
        yield* waitForRuntimeIdleStop(threadId);
        return yield* lifecycle.run(threadId, (lease) =>
          Effect.gen(function* () {
        const persistedBinding = Option.getOrUndefined(yield* directory.getBinding(threadId));
        const resolved = yield* resolveLaunchProviderInstance({
          operation: "ProviderService.startSession",
          provider: parsed.provider,
          providerInstanceId:
            input.providerInstanceId ??
            input.modelSelection?.instanceId ??
            (persistedBinding?.provider === input.provider
              ? persistedBinding.providerInstanceId
              : undefined),
          ...(input.modelSelection ? { modelSelection: input.modelSelection } : {}),
          ...(input.providerOptions ? { providerOptions: input.providerOptions } : {}),
        });
        const effectiveProviderOptions = resolved.providerOptions;
        const hasExplicitResumeCursor = input.resumeCursor !== undefined;
        const currentContinuationIdentity =
          (hasExplicitResumeCursor && resolved.instance.driver === "codex") ||
          (persistedBinding !== undefined && persistedBinding.provider === resolved.instance.driver)
            ? yield* prepareContinuationIdentityForCompatibility({
                operation: "ProviderService.startSession",
                provider: resolved.instance.driver,
                providerOptions: effectiveProviderOptions,
                persistedIdentity: readPersistedContinuationIdentity(
                  persistedBinding?.runtimePayload,
                ),
                ...(hasExplicitResumeCursor && resolved.instance.driver === "codex"
                  ? { explicitResume: true }
                  : {}),
              })
            : undefined;
        const exactPersistedLaunchMatch =
          persistedBinding !== undefined &&
          persistedLaunchMatchesExactly({
            binding: persistedBinding,
            provider: resolved.instance.driver,
            providerInstanceId: resolved.instance.instanceId,
            providerOptions: effectiveProviderOptions,
            credentialsFingerprintKey,
          });
        const continuationCompatible =
          persistedBinding !== undefined &&
          persistedContinuationMatchesLaunch({
            binding: persistedBinding,
            provider: resolved.instance.driver,
            providerInstanceId: resolved.instance.instanceId,
            providerOptions: effectiveProviderOptions,
            credentialsFingerprintKey,
            currentIdentity: currentContinuationIdentity,
          });
        const hasAvailableResumeCursor =
          input.resumeCursor !== undefined || hasResumeCursor(persistedBinding?.resumeCursor);
        const continuationResetRequested =
          persistedBinding !== undefined &&
          readPersistedContinuationResetRequested(persistedBinding.runtimePayload);
        const canReusePersistedResumeCursor =
          persistedBinding !== undefined &&
          hasResumeCursor(persistedBinding.resumeCursor) &&
          continuationCompatible;
        if (
          persistedBinding !== undefined &&
          !continuationResetRequested &&
          ((!hasAvailableResumeCursor && !exactPersistedLaunchMatch) ||
            (hasAvailableResumeCursor && !continuationCompatible)) &&
          (providerUsesProtectedNativeContinuation(persistedBinding.provider) ||
            providerUsesProtectedNativeContinuation(resolved.instance.driver))
        ) {
          yield* Effect.sync(() => scheduleRuntimeIdleStop(threadId));
          return yield* toValidationError(
            "ProviderService.startSession",
            incompatibleContinuationMessage({
              threadId,
              previousProvider: persistedBinding.provider,
              previousInstanceId: providerInstanceIdFromBinding(persistedBinding),
              nextProvider: resolved.instance.driver,
              nextInstanceId: resolved.instance.instanceId,
            }),
          );
        }
        const effectiveResumeCursor =
          input.resumeCursor ??
          (canReusePersistedResumeCursor ? persistedBinding?.resumeCursor : undefined);
        const expectedCodexContinuationGeneration =
          effectiveResumeCursor !== undefined && resolved.instance.driver === "codex"
            ? codexSharedContinuationGeneration(currentContinuationIdentity)
            : undefined;
        if (
          effectiveResumeCursor !== undefined &&
          resolved.instance.driver === "codex" &&
          expectedCodexContinuationGeneration === undefined
        ) {
          yield* Effect.sync(() => scheduleRuntimeIdleStop(threadId));
          return yield* toValidationError(
            "ProviderService.startSession",
            "Cannot resume a Codex native thread because the selected continuation source has no verified generation.",
          );
        }
        const adapter = yield* getAdapterForInstance(resolved.instance);
        yield* stopStaleSessionsForThread({
          threadId,
          provider: resolved.instance.driver,
          providerInstanceId: resolved.instance.instanceId,
        });
        const session = yield* adapter.startSession({
          ...input,
          provider: resolved.instance.driver,
          providerInstanceId: resolved.instance.instanceId,
          lifecycleGeneration: lease.generation,
          ...(resolved.modelSelection !== undefined
            ? { modelSelection: resolved.modelSelection }
            : {}),
          ...(effectiveProviderOptions !== undefined
            ? { providerOptions: effectiveProviderOptions }
            : {}),
          ...(effectiveResumeCursor !== undefined ? { resumeCursor: effectiveResumeCursor } : {}),
          ...(expectedCodexContinuationGeneration ? { expectedCodexContinuationGeneration } : {}),
        });

              if (session.provider !== adapter.provider) {
                return yield* toValidationError(
                  "ProviderService.startSession",
                  `Adapter/provider mismatch: requested '${adapter.provider}', received '${session.provider}'.`,
                );
              }

        const sessionWithInstance: ProviderSession = {
          ...session,
          providerInstanceId: resolved.instance.instanceId,
        };
        yield* upsertSessionBinding(sessionWithInstance, threadId, {
          lifecycleGeneration: lease.generation,
          modelSelection: resolved.modelSelection,
          providerOptions: effectiveProviderOptions,
          providerInstanceId: resolved.instance.instanceId,
          launchOptionsAuthoritative: true,
        });
        yield* analytics.record("provider.session.started", {
          provider: session.provider,
          providerInstanceId: resolved.instance.instanceId,
          runtimeMode: input.runtimeMode,
          hasResumeCursor: hasResumeCursor(session.resumeCursor),
          hasCwd: typeof input.cwd === "string" && input.cwd.trim().length > 0,
          hasModel:
            typeof resolved.modelSelection?.model === "string" &&
            resolved.modelSelection.model.trim().length > 0,
        });

        return sessionWithInstance;
          }),
        );
      });

    const forkThread: NonNullable<ProviderServiceShape["forkThread"]> = (rawInput) =>
      Effect.gen(function* () {
        const input = yield* decodeInputOrValidationError({
          operation: "ProviderService.forkThread",
          schema: ProviderForkThreadInput,
          payload: rawInput,
        });

        const sourceBinding = Option.getOrUndefined(
          yield* directory.getBinding(input.sourceThreadId),
        );
        if (!sourceBinding) {
          return null;
        }

        if (Option.isSome(yield* directory.getBinding(input.threadId))) {
          return null;
        }

        const sourcePayloadProviderInstanceId = readPersistedProviderInstanceId(
          sourceBinding.runtimePayload,
        );
        const sourceBoundProviderInstanceId =
          sourceBinding.providerInstanceId ??
          sourcePayloadProviderInstanceId ??
          sourceBinding.provider;
        const requestedProviderInstanceId = input.modelSelection
          ? resolveModelSelectionInstanceId(input.modelSelection)
          : undefined;
        const sourceProviderInstanceId =
          requestedProviderInstanceId ?? sourceBoundProviderInstanceId;
        const hasSourceProviderInstanceBinding = bindingHasExplicitProviderInstance({
          binding: sourceBinding,
          persistedPayloadProviderInstanceId: sourcePayloadProviderInstanceId,
          persistedModelSelection: input.modelSelection,
        });
        const sourcePersistedProviderOptions = readPersistedProviderOptions(
          sourceBinding.runtimePayload,
        );
        const usesPersistedSourceOptions =
          input.providerOptions === undefined &&
          !hasSourceProviderInstanceBinding &&
          sourcePersistedProviderOptions !== undefined;
        const resolvedSource = yield* resolveLaunchProviderInstance({
          operation: "ProviderService.forkThread",
          providerInstanceId: sourceProviderInstanceId,
          ...(input.modelSelection !== undefined ? { modelSelection: input.modelSelection } : {}),
          providerOptions:
            input.providerOptions ??
            (hasSourceProviderInstanceBinding ? undefined : sourcePersistedProviderOptions),
          ...(usesPersistedSourceOptions ? { providerOptionsPrecedence: "caller" as const } : {}),
        });
        if (resolvedSource.instance.driver !== sourceBinding.provider) {
          yield* Effect.logInfo(
            "provider native fork skipped because requested instance uses another provider",
            {
              sourceThreadId: input.sourceThreadId,
              threadId: input.threadId,
              sourceProvider: sourceBinding.provider,
              sourceProviderInstanceId: sourceBoundProviderInstanceId,
              requestedProvider: resolvedSource.instance.driver,
              requestedProviderInstanceId: resolvedSource.instance.instanceId,
            },
          );
          return null;
        }
        const effectiveProviderOptions = resolvedSource.providerOptions;
        const hasSourceResumeCursor = hasResumeCursor(sourceBinding.resumeCursor);
        const currentContinuationIdentity = hasSourceResumeCursor
          ? yield* prepareContinuationIdentityForCompatibility({
              operation: "ProviderService.forkThread",
              provider: resolvedSource.instance.driver,
              providerOptions: effectiveProviderOptions,
              persistedIdentity: readPersistedContinuationIdentity(sourceBinding.runtimePayload),
            })
          : undefined;
        const canReuseSourceResumeCursor =
          hasSourceResumeCursor &&
          persistedContinuationMatchesLaunch({
            binding: sourceBinding,
            provider: resolvedSource.instance.driver,
            providerInstanceId: resolvedSource.instance.instanceId,
            providerOptions: effectiveProviderOptions,
            credentialsFingerprintKey,
            currentIdentity: currentContinuationIdentity,
          });
        const expectedCodexContinuationGeneration = canReuseSourceResumeCursor
          ? codexSharedContinuationGeneration(currentContinuationIdentity)
          : undefined;
        if (
          canReuseSourceResumeCursor &&
          resolvedSource.instance.driver === "codex" &&
          expectedCodexContinuationGeneration === undefined
        ) {
          yield* Effect.logInfo(
            "provider native fork skipped because source continuation has no verified generation",
            {
              sourceThreadId: input.sourceThreadId,
              threadId: input.threadId,
              sourceProviderInstanceId: sourceBoundProviderInstanceId,
              requestedProviderInstanceId: resolvedSource.instance.instanceId,
            },
          );
          return null;
        }
        if (
          resolvedSource.instance.instanceId !== sourceBoundProviderInstanceId &&
          !canReuseSourceResumeCursor
        ) {
          yield* Effect.logInfo(
            "provider native fork skipped because requested instance cannot reuse source continuation",
            {
              sourceThreadId: input.sourceThreadId,
              threadId: input.threadId,
              sourceProviderInstanceId: sourceBoundProviderInstanceId,
              requestedProviderInstanceId: resolvedSource.instance.instanceId,
            },
          );
          return null;
        }
        if (
          hasSourceResumeCursor &&
          providerUsesProtectedNativeContinuation(resolvedSource.instance.driver) &&
          !canReuseSourceResumeCursor
        ) {
          yield* Effect.logInfo(
            "provider native fork skipped because source continuation storage is incompatible",
            {
              sourceThreadId: input.sourceThreadId,
              threadId: input.threadId,
              sourceProviderInstanceId: sourceBoundProviderInstanceId,
              requestedProviderInstanceId: resolvedSource.instance.instanceId,
            },
          );
          return null;
        }
        const sourceCwd = readPersistedCwd(sourceBinding.runtimePayload);

        const adapter = yield* getAdapterForInstance(resolvedSource.instance);
        if (!adapter.forkThread) {
          return null;
        }

        const forked = yield* adapter
          .forkThread({
            ...input,
            threadId: input.threadId,
            sourceThreadId: input.sourceThreadId,
            ...(resolvedSource.modelSelection !== undefined
              ? { modelSelection: resolvedSource.modelSelection }
              : {}),
            ...(effectiveProviderOptions !== undefined
              ? { providerOptions: effectiveProviderOptions }
              : {}),
            ...(canReuseSourceResumeCursor
              ? { sourceResumeCursor: sourceBinding.resumeCursor }
              : {}),
            ...(expectedCodexContinuationGeneration ? { expectedCodexContinuationGeneration } : {}),
            ...(sourceCwd ? { sourceCwd } : {}),
            runtimeMode: input.runtimeMode,
          })
          .pipe(
            Effect.catch((error) =>
              Effect.logWarning("provider native fork failed; falling back", {
                sourceThreadId: input.sourceThreadId,
                targetThreadId: input.threadId,
                cause: error instanceof Error ? error.message : String(error),
              }).pipe(Effect.as(null)),
            ),
          );
        if (!forked) {
          return null;
        }

        const forkedSession = (yield* adapter.listSessions()).find(
          (session) => session.threadId === input.threadId,
        );
        if (forkedSession) {
          const forkedSessionWithInstance: ProviderSession = {
            ...forkedSession,
            providerInstanceId: resolvedSource.instance.instanceId,
          };
          yield* upsertSessionBinding(forkedSessionWithInstance, input.threadId, {
            ...(resolvedSource.modelSelection !== undefined
              ? { modelSelection: resolvedSource.modelSelection }
              : {}),
            ...(effectiveProviderOptions !== undefined
              ? { providerOptions: effectiveProviderOptions }
              : {}),
            providerInstanceId: resolvedSource.instance.instanceId,
            lastRuntimeEvent: "provider.thread.forked",
            lastRuntimeEventAt: new Date().toISOString(),
            launchOptionsAuthoritative: true,
          });
        } else {
          const forkedAt = new Date().toISOString();
          const stoppedForkSession: ProviderSession = {
            provider: adapter.provider,
            providerInstanceId: resolvedSource.instance.instanceId,
            runtimeMode: input.runtimeMode,
            status: "closed",
            ...(input.cwd ? { cwd: input.cwd } : {}),
            ...(resolvedSource.modelSelection?.model
              ? { model: resolvedSource.modelSelection.model }
              : {}),
            threadId: input.threadId,
            ...(forked.resumeCursor !== undefined ? { resumeCursor: forked.resumeCursor } : {}),
            createdAt: forkedAt,
            updatedAt: forkedAt,
          };
          yield* upsertSessionBinding(stoppedForkSession, input.threadId, {
            ...(resolvedSource.modelSelection !== undefined
              ? { modelSelection: resolvedSource.modelSelection }
              : {}),
            ...(effectiveProviderOptions !== undefined
              ? { providerOptions: effectiveProviderOptions }
              : {}),
            providerInstanceId: resolvedSource.instance.instanceId,
            lastRuntimeEvent: "provider.thread.forked",
            lastRuntimeEventAt: forkedAt,
            launchOptionsAuthoritative: true,
          });
        }
        yield* analytics.record("provider.thread.forked", {
          provider: adapter.provider,
          providerInstanceId: resolvedSource.instance.instanceId,
        });
        return forked;
      });

    const sendTurn: ProviderServiceShape["sendTurn"] = (rawInput) =>
      Effect.gen(function* () {
        const parsed = yield* decodeInputOrValidationError({
          operation: "ProviderService.sendTurn",
          schema: ProviderSendTurnInput,
          payload: rawInput,
        });

        const input = {
          ...parsed,
          attachments: carryProviderAttachmentPaths(rawInput, parsed.attachments ?? []),
        };
        if (!input.input && input.attachments.length === 0) {
          return yield* toValidationError(
            "ProviderService.sendTurn",
            "Either input text or at least one attachment is required",
          );
        }
        return yield* runTurnDispatch(input.threadId, (generation) =>
          Effect.gen(function* () {
            const routed = yield* resolveRoutableSession({
              threadId: input.threadId,
              operation: "ProviderService.sendTurn",
              allowRecovery: true,
            });
            const routeValidationError = validateModelSelectionMatchesRoute({
              operation: "ProviderService.sendTurn",
              modelSelection: input.modelSelection,
              provider: routed.adapter.provider,
              providerInstanceId: routed.providerInstanceId,
            });
            if (routeValidationError) {
              return yield* routeValidationError;
            }
            const routedModelSelection = modelSelectionForRoute(
              input.modelSelection,
              routed.providerInstanceId,
            );
            const turn = yield* routed.adapter.sendTurn({
              ...input,
              ...(routedModelSelection !== undefined
                ? { modelSelection: routedModelSelection }
                : {}),
            });
            const persistenceInput: StartedTurnPersistenceInput = {
              threadId: input.threadId,
              provider: routed.adapter.provider,
              providerInstanceId: routed.providerInstanceId as ProviderInstanceId,
              turnId: String(turn.turnId),
              generation,
              ...(turn.resumeCursor !== undefined ? { resumeCursor: turn.resumeCursor } : {}),
              ...(routedModelSelection !== undefined
                ? { modelSelection: routedModelSelection }
                : {}),
              lastRuntimeEvent: "provider.sendTurn",
            };
            rememberSuccessfulTurnDispatch(persistenceInput);
            yield* persistStartedTurn(persistenceInput);
            yield* analytics.record("provider.turn.sent", {
              provider: routed.adapter.provider,
              model: routedModelSelection?.model,
              interactionMode: input.interactionMode,
              attachmentCount: input.attachments.length,
              hasInput: typeof input.input === "string" && input.input.trim().length > 0,
            });
            return turn;
          }),
        );
      });

    const steerTurn: ProviderServiceShape["steerTurn"] = (rawInput) =>
      Effect.gen(function* () {
        const parsed = yield* decodeInputOrValidationError({
          operation: "ProviderService.steerTurn",
          schema: ProviderSteerTurnInput,
          payload: rawInput,
        });

        const input = {
          ...parsed,
          attachments: carryProviderAttachmentPaths(rawInput, parsed.attachments ?? []),
        };
        if (!input.input && input.attachments.length === 0) {
          return yield* toValidationError(
            "ProviderService.steerTurn",
            "Either input text or at least one attachment is required",
          );
        }
        return yield* runTurnDispatch(input.threadId, (generation) =>
          Effect.gen(function* () {
            const routed = yield* resolveRoutableSession({
              threadId: input.threadId,
              operation: "ProviderService.steerTurn",
              allowRecovery: true,
            });
            if (
              !routed.adapter.steerTurn ||
              routed.adapter.capabilities.supportsTurnSteering !== true
            ) {
              return yield* toValidationError(
                "ProviderService.steerTurn",
                `Provider '${routed.adapter.provider}' does not support steering an active turn.`,
              );
            }
            const routeValidationError = validateModelSelectionMatchesRoute({
              operation: "ProviderService.steerTurn",
              modelSelection: input.modelSelection,
              provider: routed.adapter.provider,
              providerInstanceId: routed.providerInstanceId,
            });
            if (routeValidationError) {
              return yield* routeValidationError;
            }
            const routedModelSelection = modelSelectionForRoute(
              input.modelSelection,
              routed.providerInstanceId,
            );
            const turn = yield* routed.adapter.steerTurn({
              ...input,
              ...(routedModelSelection !== undefined
                ? { modelSelection: routedModelSelection }
                : {}),
            });
            const persistenceInput: StartedTurnPersistenceInput = {
              threadId: input.threadId,
              provider: routed.adapter.provider,
              providerInstanceId: routed.providerInstanceId as ProviderInstanceId,
              turnId: String(turn.turnId),
              generation,
              ...(turn.resumeCursor !== undefined ? { resumeCursor: turn.resumeCursor } : {}),
              ...(routedModelSelection !== undefined
                ? { modelSelection: routedModelSelection }
                : {}),
              lastRuntimeEvent: "provider.steerTurn",
            };
            rememberSuccessfulTurnDispatch(persistenceInput);
            yield* persistStartedTurn(persistenceInput);
            yield* analytics.record("provider.turn.steered", {
              provider: routed.adapter.provider,
              model: routedModelSelection?.model,
              interactionMode: input.interactionMode,
              attachmentCount: input.attachments.length,
              hasInput: typeof input.input === "string" && input.input.trim().length > 0,
            });
            return turn;
          }),
        );
      });

    const startReview: ProviderServiceShape["startReview"] = (rawInput) =>
      Effect.gen(function* () {
        const input = yield* decodeInputOrValidationError({
          operation: "ProviderService.startReview",
          schema: ProviderStartReviewInput,
          payload: rawInput,
        });

        return yield* runTurnDispatch(input.threadId, (generation) =>
          Effect.gen(function* () {
            const routed = yield* resolveRoutableSession({
              threadId: input.threadId,
              operation: "ProviderService.startReview",
              allowRecovery: true,
            });
            if (!routed.adapter.startReview) {
              return yield* toValidationError(
                "ProviderService.startReview",
                `Provider '${routed.adapter.provider}' does not support native review.`,
              );
            }

            const turn = yield* routed.adapter.startReview(input);
            const persistenceInput: StartedTurnPersistenceInput = {
              threadId: input.threadId,
              provider: routed.adapter.provider,
              providerInstanceId: routed.providerInstanceId as ProviderInstanceId,
              turnId: String(turn.turnId),
              generation,
              ...(turn.resumeCursor !== undefined ? { resumeCursor: turn.resumeCursor } : {}),
              lastRuntimeEvent: "provider.startReview",
            };
            rememberSuccessfulTurnDispatch(persistenceInput);
            yield* persistStartedTurn(persistenceInput);
            yield* analytics.record("provider.review.started", {
              provider: routed.adapter.provider,
              target: input.target.type,
            });
            return turn;
          }),
        );
      });

    const interruptTurn: ProviderServiceShape["interruptTurn"] = (rawInput) =>
      Effect.gen(function* () {
        const input = yield* decodeInputOrValidationError({
          operation: "ProviderService.interruptTurn",
          schema: ProviderInterruptTurnInput,
          payload: rawInput,
        });
        return yield* lifecycle.runCurrent(input.threadId, (currentGeneration) =>
          Effect.gen(function* () {
            const routed = yield* resolveRoutableSession({
              threadId: input.threadId,
              operation: "ProviderService.interruptTurn",
              allowRecovery: false,
            });
            if (!routed.isActive) {
              return yield* toValidationError(
                "ProviderService.interruptTurn",
                `Cannot interrupt thread '${input.threadId}' because its provider runtime is not active.`,
              );
            }

            const binding = Option.getOrUndefined(yield* directory.getBinding(input.threadId));
            if (!binding) {
              return yield* toValidationError(
                "ProviderService.interruptTurn",
                `Cannot interrupt thread '${input.threadId}' without a persisted provider binding.`,
              );
            }
            const bindingGeneration = binding.lifecycleGeneration ?? currentGeneration;
            if (
              currentGeneration !== undefined &&
              bindingGeneration !== undefined &&
              bindingGeneration !== currentGeneration
            ) {
              return yield* toValidationError(
                "ProviderService.interruptTurn",
                `Cannot interrupt stale provider generation '${bindingGeneration}' for thread '${input.threadId}'.`,
              );
            }

            const boundActiveTurnId = runtimeActiveTurnId(binding.runtimePayload);
            const providerTurnId =
              input.providerThreadId !== undefined ? input.turnId : boundActiveTurnId;
            if (providerTurnId === undefined) {
              return yield* toValidationError(
                "ProviderService.interruptTurn",
                `Cannot interrupt thread '${input.threadId}' because no exact active provider turn is bound.`,
              );
            }
            if (
              input.providerThreadId === undefined &&
              input.turnId !== undefined &&
              input.turnId !== providerTurnId
            ) {
              return yield* toValidationError(
                "ProviderService.interruptTurn",
                `Cannot interrupt stale turn '${input.turnId}' because '${providerTurnId}' is active.`,
              );
            }

            yield* routed.adapter.interruptTurn(
              input.threadId,
              TurnId.makeUnsafe(providerTurnId),
              input.providerThreadId,
            );
            yield* analytics.record("provider.turn.interrupted", {
              provider: routed.adapter.provider,
            });
          }),
        );
      });

    const respondToInteraction = (response: InteractionResponse) => {
      const { input } = response;
      const operation =
        response.kind === "approval"
          ? "ProviderService.respondToRequest"
          : "ProviderService.respondToUserInput";
      return lifecycle.runCurrent(input.threadId, (currentGeneration) =>
        Effect.gen(function* () {
          const routed = yield* resolveRoutableSession({
            threadId: input.threadId,
            operation,
            allowRecovery: false,
          });
          if (!routed.isActive) {
            return yield* toValidationError(
              operation,
              `Cannot respond to request '${input.requestId}' because the provider runtime is not active.`,
            );
          }
          const routedGeneration = routed.lifecycleGeneration ?? currentGeneration;
          if (
            routedGeneration !== undefined &&
            routedGeneration !== "legacy" &&
            input.lifecycleGeneration === undefined
          ) {
            return yield* toValidationError(
              operation,
              `Cannot respond to request '${input.requestId}' without its provider lifecycle generation.`,
            );
          }
          if (
            input.lifecycleGeneration !== undefined &&
            input.lifecycleGeneration !== routedGeneration
          ) {
            return yield* toValidationError(
              operation,
              `Cannot respond to stale request '${input.requestId}' from provider generation '${input.lifecycleGeneration}'.`,
            );
          }
          if (response.kind === "approval") {
            yield* routed.adapter.respondToRequest(
              input.threadId,
              input.requestId,
              response.input.decision,
            );
            yield* analytics.record("provider.request.responded", {
              provider: routed.adapter.provider,
              decision: response.input.decision,
            });
            return;
          }
          yield* routed.adapter.respondToUserInput(
            input.threadId,
            input.requestId,
            response.input.answers,
          );
        }),
      );
    };

    const respondToRequest: ProviderServiceShape["respondToRequest"] = (rawInput) =>
      decodeInputOrValidationError({
        operation: "ProviderService.respondToRequest",
        schema: ProviderRespondToRequestInput,
        payload: rawInput,
      }).pipe(Effect.flatMap((input) => respondToInteraction({ kind: "approval", input })));

    const respondToUserInput: ProviderServiceShape["respondToUserInput"] = (rawInput) =>
      decodeInputOrValidationError({
        operation: "ProviderService.respondToUserInput",
        schema: ProviderRespondToUserInputInput,
        payload: rawInput,
      }).pipe(Effect.flatMap((input) => respondToInteraction({ kind: "userInput", input })));

    const stopSession: ProviderServiceShape["stopSession"] = (rawInput) =>
      Effect.gen(function* () {
        const input = yield* decodeInputOrValidationError({
          operation: "ProviderService.stopSession",
          schema: ProviderStopSessionInput,
          payload: rawInput,
        });
        yield* waitForRuntimeIdleStop(input.threadId);
        clearRuntimeIdleTimer(input.threadId);
        return yield* lifecycle.run(input.threadId, (lease) =>
          Effect.gen(function* () {
            const routed = yield* resolveRoutableSession({
              threadId: input.threadId,
              operation: "ProviderService.stopSession",
              allowRecovery: false,
              allowDisabled: true,
              allowDeleted: true,
            });
            if (routed.isActive) {
              yield* routed.adapter.stopSession(routed.threadId);
            }
            yield* waitForRuntimeIdleStop(input.threadId);
            yield* withBindingWriteLock(input.threadId, directory.remove(input.threadId));
            lease.retire();
            retireRuntimeIdleGeneration(input.threadId);
            yield* analytics.record("provider.session.stopped", {
              provider: routed.adapter.provider,
            });
          }),
        );
      });

    const stopRuntimeSessionInternal = (
      rawInput: StopRuntimeSessionInput,
      expectedIdleGeneration?: symbol,
    ): StopRuntimeSessionEffect =>
      Effect.gen(function* () {
        const input = yield* decodeInputOrValidationError({
          operation: "ProviderService.stopRuntimeSession",
          schema: ProviderStopSessionInput,
          payload: rawInput,
        });
        const isExpectedIdleStopCurrent = () =>
          expectedIdleGeneration === undefined ||
          isRuntimeIdleGenerationCurrent(input.threadId, expectedIdleGeneration);
        if (expectedIdleGeneration === undefined) {
          yield* waitForRuntimeIdleStop(input.threadId);
          clearRuntimeIdleTimer(input.threadId);
        } else if (!isExpectedIdleStopCurrent()) {
          return;
        }
        return yield* lifecycle.run(input.threadId, (lease) =>
          Effect.gen(function* () {
            if (!isExpectedIdleStopCurrent()) {
              return;
            }
            const binding = Option.getOrUndefined(yield* directory.getBinding(input.threadId));
            if (!binding || !isExpectedIdleStopCurrent()) {
              return;
            }
            const adapter = yield* getAdapterForBinding(binding);
            const hasActiveSession = yield* adapter.hasSession(input.threadId);
            if (!isExpectedIdleStopCurrent()) {
              return;
            }
            if (hasActiveSession) {
              yield* adapter.stopSession(input.threadId);
            }
            if (!isExpectedIdleStopCurrent()) {
              return;
            }
            yield* withBindingWriteLock(
              input.threadId,
              directory.upsert({
                threadId: input.threadId,
                provider: binding.provider,
                providerInstanceId: binding.providerInstanceId,
                ...(binding.adapterKey !== undefined ? { adapterKey: binding.adapterKey } : {}),
                ...(binding.runtimeMode !== undefined ? { runtimeMode: binding.runtimeMode } : {}),
                status: "stopped",
                lifecycleGeneration: lease.generation,
                resumeCursor: binding.resumeCursor,
                runtimePayload: {
                  ...runtimePayloadRecord(binding.runtimePayload),
                  activeTurnId: null,
                  lifecycleGeneration: lease.generation,
                  lastRuntimeEvent: "provider.stopRuntimeSession",
                  lastRuntimeEventAt: new Date().toISOString(),
                },
              }),
            );
            yield* analytics.record("provider.session.runtime_stopped", {
              provider: binding.provider,
            });
            retireRuntimeIdleGeneration(input.threadId, expectedIdleGeneration);
          }),
        );
      });

    const stopRuntimeSession: StopRuntimeSession = (rawInput) =>
      stopRuntimeSessionInternal(rawInput);

    stopIdleRuntimeSession = (threadId, generation) => {
      const stopEffect = Effect.gen(function* () {
        const binding = Option.getOrUndefined(yield* directory.getBinding(threadId));
        if (!binding) {
          retireRuntimeIdleGeneration(threadId, generation);
          return;
        }

        const adapter = yield* getAdapterForBinding(binding);
        const sessions = yield* adapter.listSessions();
        const session = sessions.find((entry) => entry.threadId === threadId);
        const bindingRuntimePayload = runtimePayloadRecord(binding.runtimePayload);
        if (
          bindingRuntimePayload.activeTurnId !== null &&
          bindingRuntimePayload.activeTurnId !== undefined
        ) {
          retireRuntimeIdleGeneration(threadId, generation);
          return;
        }
        const isIdleReadySession =
          session?.status === "ready" ||
          (session?.status === "running" &&
            binding.status === "stopped" &&
            (bindingRuntimePayload.lastRuntimeEvent === "thread.state.changed" ||
              bindingRuntimePayload.lastRuntimeEvent === "provider.compactThread"));
        if (!session || !isIdleReadySession || session.activeTurnId !== undefined) {
          retireRuntimeIdleGeneration(threadId, generation);
          return;
        }
        // Live adapter snapshots can temporarily omit cursors even though the
        // directory already persisted one from an earlier runtime event.
        if (!hasResumeCursor(session.resumeCursor) && !hasResumeCursor(binding.resumeCursor)) {
          retireRuntimeIdleGeneration(threadId, generation);
          return;
        }
        if (!isRuntimeIdleGenerationCurrent(threadId, generation)) {
          return;
        }

        yield* stopRuntimeSessionInternal({ threadId }, generation);
      }).pipe(
        Effect.catchCause((cause) =>
          Effect.logWarning("provider.session.idle_stop_failed", {
            threadId,
            cause,
          }),
        ),
      );
      const stopPromise = Effect.runPromise(stopEffect).finally(() => {
        if (runtimeIdleStopsInFlight.get(threadId) === stopPromise) {
          runtimeIdleStopsInFlight.delete(threadId);
        }
      });
      runtimeIdleStopsInFlight.set(threadId, stopPromise);
    };

    const clearSessionResumeCursor: NonNullable<
      ProviderServiceShape["clearSessionResumeCursor"]
    > = (rawInput) =>
      Effect.gen(function* () {
        const input = yield* decodeInputOrValidationError({
          operation: "ProviderService.clearSessionResumeCursor",
          schema: ProviderStopSessionInput,
          payload: rawInput,
        });
        yield* waitForRuntimeIdleStop(input.threadId);
        clearRuntimeIdleTimer(input.threadId);
        return yield* lifecycle.run(input.threadId, (lease) =>
          Effect.gen(function* () {
            const binding = Option.getOrUndefined(yield* directory.getBinding(input.threadId));
            if (!binding) {
              return;
            }
            const adapter = yield* getAdapterForBinding(binding);
            const hasActiveSession = yield* adapter.hasSession(input.threadId);
            if (hasActiveSession) {
              yield* adapter.stopSession(input.threadId);
            }
            yield* waitForRuntimeIdleStop(input.threadId);
            yield* withBindingWriteLock(
              input.threadId,
              directory.upsert({
                threadId: input.threadId,
                provider: binding.provider,
                providerInstanceId: binding.providerInstanceId,
                ...(binding.adapterKey !== undefined ? { adapterKey: binding.adapterKey } : {}),
                ...(binding.runtimeMode !== undefined ? { runtimeMode: binding.runtimeMode } : {}),
                status: "stopped",
                lifecycleGeneration: lease.generation,
                resumeCursor: null,
                runtimePayload: {
                  ...runtimePayloadRecord(binding.runtimePayload),
                  continuationResetRequested: true,
                  lifecycleGeneration: lease.generation,
                },
              }),
            );
            yield* analytics.record("provider.session.resume_cursor_cleared", {
              provider: binding.provider,
            });
            retireRuntimeIdleGeneration(input.threadId);
          }),
        );
      });

    const listSessions: ProviderServiceShape["listSessions"] = () =>
      Effect.gen(function* () {
        const activeSessions = (yield* Effect.forEach(adapters, (adapter) =>
          adapter.listSessions(),
        )).flatMap((sessions) => sessions);
        const persistedBindings = yield* directory.listThreadIds().pipe(
          Effect.flatMap((threadIds) =>
            Effect.forEach(
              threadIds,
              (threadId) =>
                directory
                  .getBinding(threadId)
                  .pipe(Effect.orElseSucceed(() => Option.none<ProviderRuntimeBinding>())),
              { concurrency: "unbounded" },
            ),
          ),
          Effect.orElseSucceed(() => [] as Array<Option.Option<ProviderRuntimeBinding>>),
        );
        const bindingsByThreadId = new Map(
          EffectArray.getSomes(persistedBindings).map(
            (binding) => [binding.threadId, binding] as const,
          ),
        );

        return activeSessions.map((session) => {
          const binding = bindingsByThreadId.get(session.threadId);
          if (!binding || binding.provider !== session.provider) {
            return session;
          }
          const bindingProviderInstanceId =
            binding.providerInstanceId ?? readPersistedProviderInstanceId(binding.runtimePayload);
          if (
            session.providerInstanceId !== undefined &&
            bindingProviderInstanceId !== undefined &&
            session.providerInstanceId !== bindingProviderInstanceId
          ) {
            return session;
          }

          const overrides: {
            resumeCursor?: ProviderSession["resumeCursor"];
            runtimeMode?: ProviderSession["runtimeMode"];
            providerInstanceId?: ProviderSession["providerInstanceId"];
          } = {};
          if (session.resumeCursor === undefined && binding.resumeCursor !== undefined) {
            overrides.resumeCursor = binding.resumeCursor;
          }
          if (binding.runtimeMode !== undefined) {
            overrides.runtimeMode = binding.runtimeMode;
          }
          if (bindingProviderInstanceId !== undefined) {
            overrides.providerInstanceId = bindingProviderInstanceId;
          }
          return Object.assign({}, session, overrides);
        });
      });

    const getCapabilities: ProviderServiceShape["getCapabilities"] = (instanceId) =>
      Effect.gen(function* () {
        const settings = yield* serverSettings.getSettings.pipe(
          Effect.mapError((cause) =>
            toValidationError(
              "ProviderService.getCapabilities",
              "Failed to load provider instance settings.",
              cause,
            ),
          ),
        );
        const instance = resolveProviderInstance(settings, { instanceId });
        if (!instance) {
          return yield* toValidationError(
            "ProviderService.getCapabilities",
            `Provider instance '${instanceId}' is not configured.`,
          );
        }
        if (!instance.enabled) {
          return yield* toValidationError(
            "ProviderService.getCapabilities",
            `Provider instance '${instanceId}' is disabled.`,
          );
        }
        const adapter = yield* getAdapterForInstance(instance);
        return adapter.capabilities;
      });

    const rollbackConversation: ProviderServiceShape["rollbackConversation"] = (rawInput) =>
      Effect.gen(function* () {
        const input = yield* decodeInputOrValidationError({
          operation: "ProviderService.rollbackConversation",
          schema: ProviderRollbackConversationInput,
          payload: rawInput,
        });
        if (input.numTurns === 0) {
          return;
        }
        yield* runIdleSensitiveProviderWork(
          input.threadId,
          Effect.gen(function* () {
            const routed = yield* resolveRoutableSession({
              threadId: input.threadId,
              operation: "ProviderService.rollbackConversation",
              // Restart-based rollback only needs the persisted binding and must
              // not replay the stale native cursor merely to close it again.
              allowRecovery: false,
            });
            if (routed.adapter.capabilities.conversationRollback === "restart-session") {
              // Some provider protocols can resume but cannot rewind. Clear their
              // native cursor so edit-and-resend cannot continue from stale history;
              // ProviderCommandReactor bootstraps the retained transcript next turn.
              yield* clearSessionResumeCursor({ threadId: input.threadId });
            } else {
              const active = routed.isActive
                ? routed
                : yield* resolveRoutableSession({
                    threadId: input.threadId,
                    operation: "ProviderService.rollbackConversation",
                    allowRecovery: true,
                  });
              yield* active.adapter.rollbackThread(input.threadId, input.numTurns);
            }
            yield* analytics.record("provider.conversation.rolled_back", {
              provider: routed.adapter.provider,
              turns: input.numTurns,
            });
          }),
          { scheduleIdleStopOnSuccess: true },
        );
      });

    const compactThread: ProviderServiceShape["compactThread"] = (rawInput) =>
      Effect.gen(function* () {
        const input = yield* decodeInputOrValidationError({
          operation: "ProviderService.compactThread",
          schema: ProviderCompactThreadInput,
          payload: rawInput,
        });
        yield* runIdleSensitiveProviderWork(
          input.threadId,
          Effect.gen(function* () {
            const routed = yield* resolveRoutableSession({
              threadId: input.threadId,
              operation: "ProviderService.compactThread",
              allowRecovery: true,
            });
            if (!routed.adapter.compactThread) {
              return yield* toValidationError(
                "ProviderService.compactThread",
                `Context compaction is unavailable for provider '${routed.adapter.provider}'.`,
              );
            }
            yield* routed.adapter.compactThread(input.threadId);
            const binding = Option.getOrUndefined(yield* directory.getBinding(input.threadId));
            if (binding) {
              yield* directory.upsert({
                threadId: input.threadId,
                provider: binding.provider,
                providerInstanceId: binding.providerInstanceId,
                ...(binding.adapterKey !== undefined ? { adapterKey: binding.adapterKey } : {}),
                ...(binding.runtimeMode !== undefined ? { runtimeMode: binding.runtimeMode } : {}),
                status: "stopped",
                resumeCursor: binding.resumeCursor,
                runtimePayload: {
                  ...runtimePayloadRecord(binding.runtimePayload),
                  activeTurnId: null,
                  lastRuntimeEvent: "provider.compactThread",
                  lastRuntimeEventAt: new Date().toISOString(),
                },
              });
            }
            yield* analytics.record("provider.thread.compacted", {
              provider: routed.adapter.provider,
            });
          }),
          { scheduleIdleStopOnSuccess: true },
        );
      });

    const runStopAll = () =>
      Effect.gen(function* () {
        const stoppedAt = new Date().toISOString();
        const threadIds = yield* directory.listThreadIds();
        const activeSessionByThreadId = new Map(
          (yield* Effect.forEach(adapters, (adapter) => adapter.listSessions()))
            .flatMap((sessions) => sessions)
            .map((session) => [session.threadId, session] as const),
        );
        const hydratedActiveSessions = yield* Effect.forEach(activeSessionByThreadId.values(), (session) =>
          sessionWithPersistedProviderInstance(session),
        );
        yield* Effect.forEach(hydratedActiveSessions, (session) =>
          upsertStoppedSessionBinding(session, stoppedAt),
        ).pipe(Effect.asVoid);
        yield* Effect.forEach(threadIds, (threadId) =>
          markPersistedThreadStopped(threadId, stoppedAt),
        ).pipe(Effect.asVoid);
        yield* Effect.forEach(adapters, (adapter) => adapter.stopAll()).pipe(Effect.asVoid);
        yield* analytics.record("provider.sessions.stopped_all", {
          sessionCount: threadIds.length,
        });
        yield* analytics.flush;
      });

    const awaitRuntimeEventFanoutDrained: Effect.Effect<void> = Effect.suspend(() =>
      PubSub.isEmpty(runtimeEventPubSub).pipe(
        Effect.flatMap((empty) =>
          empty
            ? Effect.void
            : Effect.yieldNow.pipe(Effect.andThen(awaitRuntimeEventFanoutDrained)),
        ),
      ),
    );

    const closeRuntimeEvents = yield* Effect.cached(
      Effect.uninterruptible(
        Effect.sync(() => {
          for (const timer of runtimeIdleTimers.values()) {
            clearTimeout(timer);
          }
          runtimeIdleTimers.clear();
          runtimeIdleGenerations.clear();
          runtimeIdleStopsInFlight.clear();
          stopIdleRuntimeSession = null;
        }).pipe(
          Effect.andThen(
            runStopAll().pipe(
              Effect.catchCause((cause) =>
                Effect.logWarning("failed to stop provider sessions", {
                  cause: Cause.pretty(cause),
                }),
              ),
            ),
          ),
          // Keep subscriptions alive until adapters have emitted terminal
          // events. Closing waits for an in-flight canonical event because its
          // persistence and publication section is uninterruptible.
          Effect.andThen(Scope.close(runtimeEventProducerScope, Exit.void)),
          // Downstream subscribers transfer every published event into their
          // own drainable workers before the publication owner is shut down.
          Effect.andThen(awaitRuntimeEventFanoutDrained),
          Effect.andThen(PubSub.shutdown(runtimeEventPubSub)),
        ),
      ),
    );

    yield* Effect.addFinalizer(() => closeRuntimeEvents);

    return {
      startSession,
      forkThread,
      sendTurn,
      steerTurn,
      startReview,
      interruptTurn,
      respondToRequest,
      respondToUserInput,
      stopSession,
      stopRuntimeSession,
      clearSessionResumeCursor,
      sessionBindingMatchesLaunchOptions,
      listSessions,
      getCapabilities,
      rollbackConversation,
      compactThread,
      closeRuntimeEvents,
      // Each access creates a fresh PubSub subscription so that multiple
      // consumers (ProviderRuntimeIngestion, CheckpointReactor, etc.) each
      // independently receive all runtime events.
      get streamEvents(): ProviderServiceShape["streamEvents"] {
        return Stream.fromPubSub(runtimeEventPubSub);
      },
    } satisfies ProviderServiceShape;
  });

export const ProviderServiceLive = Layer.effect(ProviderService, makeProviderService());

export function makeProviderServiceLive(options?: ProviderServiceLiveOptions) {
  return Layer.effect(ProviderService, makeProviderService(options));
}

/** Production provider service: journal each canonical event before live fan-out. */
export function makeDurableProviderServiceLive(options?: ProviderServiceLiveOptions) {
  return Layer.effect(
    ProviderService,
    Effect.gen(function* () {
      const runtimeEvents = yield* ProviderRuntimeEventRepository;
      return yield* makeProviderService({
        ...options,
        persistRuntimeEvent: (event) =>
          runtimeEvents.append(event).pipe(Effect.asVoid, Effect.orDie),
      });
    }),
  );
}
