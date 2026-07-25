import {
  DEFAULT_MODEL_BY_PROVIDER,
  type ModelSelection,
  type ProviderKind,
  type ProviderStartOptions,
  type ServerSettings,
  type ServerSettingsPatch,
} from "@synara/contracts";
import { deepMerge, type DeepPartial } from "./Struct";
import { normalizeProviderOptionSelections } from "./model";
import { defaultInstanceIdForProvider, deriveProviderInstances } from "./providerInstances";

function defaultModelForProvider(provider: ProviderKind): string {
  return provider === "pi" ? "openai/gpt-5.5" : DEFAULT_MODEL_BY_PROVIDER[provider];
}

function shouldReplaceTextGenerationModelSelection(
  patch: ServerSettingsPatch["textGenerationModelSelection"] | undefined,
): boolean {
  return Boolean(
    patch &&
    (patch.provider !== undefined || patch.instanceId !== undefined || patch.model !== undefined),
  );
}

export function applyServerSettingsPatch(
  current: ServerSettings,
  patch: ServerSettingsPatch,
): ServerSettings {
  const selectionPatch = patch.textGenerationModelSelection;
  const merged = deepMerge(current, patch as DeepPartial<ServerSettings>);
  const next: ServerSettings =
    patch.providerInstances !== undefined
      ? { ...merged, providerInstances: patch.providerInstances }
      : merged;
  if (!selectionPatch) {
    return next;
  }

  const instances = deriveProviderInstances(next);
  const currentInstance = instances.find(
    (instance) => instance.instanceId === current.textGenerationModelSelection.instanceId,
  );
  const patchedInstanceId =
    selectionPatch.instanceId ??
    (selectionPatch.provider
      ? defaultInstanceIdForProvider(selectionPatch.provider)
      : current.textGenerationModelSelection.instanceId);
  const patchedInstance =
    patchedInstanceId !== undefined
      ? instances.find((instance) => instance.instanceId === patchedInstanceId)
      : undefined;
  const provider =
    patchedInstance?.driver ?? selectionPatch.provider ?? currentInstance?.driver ?? "codex";
  const instanceId = patchedInstance?.instanceId ?? patchedInstanceId ?? provider;
  const providerChanged = provider !== (currentInstance?.driver ?? "codex");
  const model =
    selectionPatch.model ??
    (providerChanged
      ? defaultModelForProvider(provider)
      : current.textGenerationModelSelection.model);
  const options = shouldReplaceTextGenerationModelSelection(selectionPatch)
    ? selectionPatch.options
    : (selectionPatch.options ?? current.textGenerationModelSelection.options);
  const normalizedOptions = normalizeProviderOptionSelections(options);

  return {
    ...next,
    textGenerationModelSelection: {
      instanceId,
      model,
      ...(normalizedOptions !== undefined ? { options: normalizedOptions } : {}),
    } as ModelSelection,
  };
}

/** Server-owned launch options derived from the persisted non-secret settings snapshot. */
export function providerStartOptionsFromServerSettings(
  settings: ServerSettings,
): ProviderStartOptions {
  const { providers } = settings;
  return {
    codex: {
      ...(providers.codex.binaryPath ? { binaryPath: providers.codex.binaryPath } : {}),
      ...(providers.codex.homePath ? { homePath: providers.codex.homePath } : {}),
    },
    claudeAgent: {
      ...(providers.claudeAgent.binaryPath ? { binaryPath: providers.claudeAgent.binaryPath } : {}),
    },
    cursor: {
      ...(providers.cursor.binaryPath ? { binaryPath: providers.cursor.binaryPath } : {}),
      ...(providers.cursor.apiEndpoint ? { apiEndpoint: providers.cursor.apiEndpoint } : {}),
    },
    antigravity: {
      ...(providers.antigravity.binaryPath ? { binaryPath: providers.antigravity.binaryPath } : {}),
    },
    grok: {
      ...(providers.grok.binaryPath ? { binaryPath: providers.grok.binaryPath } : {}),
    },
    droid: {
      ...(providers.droid.binaryPath ? { binaryPath: providers.droid.binaryPath } : {}),
    },
    kilo: {
      ...(providers.kilo.binaryPath ? { binaryPath: providers.kilo.binaryPath } : {}),
      ...(providers.kilo.serverUrl ? { serverUrl: providers.kilo.serverUrl } : {}),
    },
    opencode: {
      ...(providers.opencode.binaryPath ? { binaryPath: providers.opencode.binaryPath } : {}),
      ...(providers.opencode.serverUrl ? { serverUrl: providers.opencode.serverUrl } : {}),
      experimentalWebSockets: providers.opencode.experimentalWebSockets,
    },
    pi: {
      ...(providers.pi.binaryPath ? { binaryPath: providers.pi.binaryPath } : {}),
      ...(providers.pi.agentDir ? { agentDir: providers.pi.agentDir } : {}),
    },
  };
}
