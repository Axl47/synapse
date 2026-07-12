import type { ProviderRuntimeBinding } from "../provider/Services/ProviderSessionDirectory";

export const ADOPTED_EXTERNAL_THREAD_RUNTIME_KEY = "adoptedExternalThread";

export function externalThreadIdFromResumeCursor(value: unknown): string | null {
  if (!value || typeof value !== "object") return null;
  const cursor = value as Record<string, unknown>;
  return typeof cursor.threadId === "string" && cursor.threadId.trim().length > 0
    ? cursor.threadId.trim()
    : null;
}

export function isAdoptedExternalThreadBinding(binding: ProviderRuntimeBinding): boolean {
  if (!binding.runtimePayload || typeof binding.runtimePayload !== "object") return false;
  return (
    (binding.runtimePayload as Record<string, unknown>)[ADOPTED_EXTERNAL_THREAD_RUNTIME_KEY] === true
  );
}
