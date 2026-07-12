import { CommandId, type ThreadId } from "@synara/contracts";
import {
  providerStartOptionsFromInstance,
  resolveProviderInstance,
} from "@synara/shared/providerInstances";
import { Effect, Option } from "effect";

import type { ProviderAdapterRegistryShape } from "../provider/Services/ProviderAdapterRegistry";
import type { ProviderSessionDirectoryShape } from "../provider/Services/ProviderSessionDirectory";
import type { ServerSettingsShape } from "../serverSettings";
import type { OrchestrationEngineShape } from "./Services/OrchestrationEngine";
import {
  externalThreadIdFromResumeCursor,
  isAdoptedExternalThreadBinding,
} from "./adoptedExternalThread";
import { mapCodexSnapshotMessages } from "./importedThreadMessages";

export interface CodexAdoptedThreadHistorySyncOptions {
  readonly orchestrationEngine: OrchestrationEngineShape;
  readonly providerAdapterRegistry: ProviderAdapterRegistryShape;
  readonly providerSessionDirectory: ProviderSessionDirectoryShape;
  readonly serverSettings: ServerSettingsShape;
}

export function makeCodexAdoptedThreadHistorySync(options: CodexAdoptedThreadHistorySyncOptions) {
  return Effect.fnUntraced(function* (threadId: ThreadId) {
    const startedAt = Date.now();
    const bindingOption = yield* options.providerSessionDirectory.getBinding(threadId);
    if (Option.isNone(bindingOption)) return false;
    const binding = bindingOption.value;
    if (binding.provider !== "codex" || !isAdoptedExternalThreadBinding(binding)) return false;

    const externalThreadId = externalThreadIdFromResumeCursor(binding.resumeCursor);
    if (!externalThreadId) return false;
    const settings = yield* options.serverSettings.getSettings;
    const instance = resolveProviderInstance(settings, {
      provider: "codex",
      instanceId: binding.providerInstanceId,
    });
    if (!instance || !instance.enabled || instance.driver !== "codex") return false;

    const adapter = options.providerAdapterRegistry.getByInstance
      ? yield* options.providerAdapterRegistry.getByInstance(instance.instanceId)
      : yield* options.providerAdapterRegistry.getByProvider("codex");
    if (!adapter.readExternalThread) return false;
    const providerOptions = providerStartOptionsFromInstance(instance);
    const snapshot = yield* adapter.readExternalThread({
      externalThreadId,
      providerInstanceId: instance.instanceId,
      ...(providerOptions ? { providerOptions } : {}),
    });
    const reconciledAt = new Date().toISOString();
    const messages = mapCodexSnapshotMessages({
      threadId,
      turns: snapshot.turns,
      importedAt: reconciledAt,
    });
    if (messages.length > 0) {
      yield* options.orchestrationEngine.dispatch({
        type: "thread.messages.import",
        commandId: CommandId.makeUnsafe(crypto.randomUUID()),
        threadId,
        messages,
        createdAt: reconciledAt,
      });
    }
    yield* Effect.logInfo("adopted Codex thread history reconciled", {
      threadId,
      externalThreadId,
      messageCount: messages.length,
      durationMs: Date.now() - startedAt,
    });
    return true;
  });
}
