import {
  DEFAULT_MODEL_BY_PROVIDER,
  type ModelSelection,
  type ProviderKind,
  type ProviderStartOptions,
  type ServerSettings,
  type ServerSettingsPatch,
} from "@synara/contracts";
import { deepMerge, type DeepPartial } from "./Struct";
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

  return {
    ...next,
    textGenerationModelSelection: {
      instanceId,
      model,
      ...(options !== undefined ? { options: options as ModelSelection["options"] } : {}),
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
      binaryPath: providers.codex.binaryPath,
      ...(providers.codex.homePath ? { homePath: providers.codex.homePath } : {}),
    },
    claudeAgent: { binaryPath: providers.claudeAgent.binaryPath },
    cursor: {
      binaryPath: providers.cursor.binaryPath,
      ...(providers.cursor.apiEndpoint ? { apiEndpoint: providers.cursor.apiEndpoint } : {}),
    },
    antigravity: { binaryPath: providers.antigravity.binaryPath },
    grok: { binaryPath: providers.grok.binaryPath },
    droid: { binaryPath: providers.droid.binaryPath },
    kilo: {
      binaryPath: providers.kilo.binaryPath,
      ...(providers.kilo.serverUrl ? { serverUrl: providers.kilo.serverUrl } : {}),
    },
    opencode: {
      binaryPath: providers.opencode.binaryPath,
      ...(providers.opencode.serverUrl ? { serverUrl: providers.opencode.serverUrl } : {}),
      experimentalWebSockets: providers.opencode.experimentalWebSockets,
    },
    pi: {
      binaryPath: providers.pi.binaryPath,
      ...(providers.pi.agentDir ? { agentDir: providers.pi.agentDir } : {}),
    },
  };
}
