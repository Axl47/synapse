import type {
  ModelSelection,
  OrchestrationSession,
  RuntimeMode,
  ThreadId,
} from "@synara/contracts";

export function deriveTurnStartModelSelection(input: {
  readonly currentModelSelection: ModelSelection;
  readonly requestedModelSelection: ModelSelection | undefined;
  readonly canAdoptRequestedProvider: boolean;
}): ModelSelection {
  const requestedModelSelection = input.requestedModelSelection;
  return requestedModelSelection !== undefined &&
    (requestedModelSelection.instanceId === input.currentModelSelection.instanceId ||
      input.canAdoptRequestedProvider)
    ? requestedModelSelection
    : input.currentModelSelection;
}

export function deriveTurnStartSession(input: {
  readonly threadId: ThreadId;
  readonly currentSession: OrchestrationSession | null;
  readonly providerName: OrchestrationSession["providerName"];
  readonly providerInstanceId: OrchestrationSession["providerInstanceId"];
  readonly requestedRuntimeMode: RuntimeMode;
  readonly requestedAt: string;
}): OrchestrationSession | null {
  if (input.currentSession?.status === "starting" || input.currentSession?.status === "running") {
    return null;
  }

  const retainCurrentBinding =
    input.currentSession !== null &&
    input.currentSession.status !== "stopped" &&
    input.currentSession.status !== "error";

  return {
    threadId: input.threadId,
    status: "starting",
    providerName: retainCurrentBinding ? input.currentSession.providerName : input.providerName,
    providerInstanceId: retainCurrentBinding
      ? (input.currentSession.providerInstanceId ?? input.providerInstanceId)
      : input.providerInstanceId,
    runtimeMode: retainCurrentBinding
      ? input.currentSession.runtimeMode
      : input.requestedRuntimeMode,
    activeTurnId: null,
    lastError: null,
    updatedAt: input.requestedAt,
  };
}
