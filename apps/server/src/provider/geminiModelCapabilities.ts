import type { ModelCapabilities } from "@synara/contracts";

export type GeminiThinkingConfigKind = "budget" | "level";
export type GeminiThinkingLevel = "LOW" | "HIGH";
export type GeminiThinkingBudget = -1 | 512 | 0;

export interface GeminiModelOptions {
  readonly thinkingLevel?: GeminiThinkingLevel;
  readonly thinkingBudget?: GeminiThinkingBudget;
}

const GEMINI_3_MODEL_PATTERN = /^(?:auto-)?gemini-3(?:[.-]|$)/i;
const GEMINI_2_5_MODEL_PATTERN = /^(?:auto-)?gemini-2\.5(?:[.-]|$)/i;
const GEMINI_THINKING_LEVEL_SET = new Set<GeminiThinkingLevel>(["LOW", "HIGH"]);
const GEMINI_THINKING_BUDGET_MAP = new Map<string, GeminiThinkingBudget>([
  ["-1", -1],
  ["0", 0],
  ["512", 512],
]);

export const DEFAULT_GEMINI_MODEL_CAPABILITIES: ModelCapabilities = {
  reasoningEffortLevels: [],
  supportsFastMode: false,
  supportsThinkingToggle: false,
  promptInjectedEffortLevels: [],
  contextWindowOptions: [],
};

export const GEMINI_3_MODEL_CAPABILITIES: ModelCapabilities = {
  reasoningEffortLevels: [
    { value: "HIGH", label: "High", isDefault: true },
    { value: "LOW", label: "Low" },
  ],
  supportsFastMode: false,
  supportsThinkingToggle: false,
  promptInjectedEffortLevels: [],
  contextWindowOptions: [],
};

export const GEMINI_2_5_MODEL_CAPABILITIES: ModelCapabilities = {
  reasoningEffortLevels: [
    { value: "-1", label: "Dynamic", isDefault: true },
    { value: "512", label: "512 Tokens" },
  ],
  supportsFastMode: false,
  supportsThinkingToggle: false,
  promptInjectedEffortLevels: [],
  contextWindowOptions: [],
};

function trimOrNull(value: string | null | undefined): string | null {
  if (typeof value !== "string") return null;
  return value.trim() || null;
}

function hasEffortLevel(capabilities: ModelCapabilities, value: string): boolean {
  return capabilities.reasoningEffortLevels.some((level) => level.value === value);
}

function isGeminiThinkingLevel(value: string): value is GeminiThinkingLevel {
  return GEMINI_THINKING_LEVEL_SET.has(value as GeminiThinkingLevel);
}

function isGeminiThinkingBudget(value: string): value is `${GeminiThinkingBudget}` {
  return GEMINI_THINKING_BUDGET_MAP.has(value);
}

function sanitizeGeminiAliasSegment(value: string): string {
  const sanitized = value
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
  return sanitized || "model";
}

export function getGeminiThinkingConfigKind(
  model: string | null | undefined,
): GeminiThinkingConfigKind | null {
  const trimmed = trimOrNull(model);
  if (!trimmed) return null;
  if (GEMINI_3_MODEL_PATTERN.test(trimmed)) return "level";
  if (GEMINI_2_5_MODEL_PATTERN.test(trimmed)) return "budget";
  return null;
}

export function geminiCapabilitiesForModel(
  model: string | null | undefined,
  fallbackCapabilities: ModelCapabilities = DEFAULT_GEMINI_MODEL_CAPABILITIES,
): ModelCapabilities {
  switch (getGeminiThinkingConfigKind(model)) {
    case "level":
      return GEMINI_3_MODEL_CAPABILITIES;
    case "budget":
      return GEMINI_2_5_MODEL_CAPABILITIES;
    default:
      return fallbackCapabilities;
  }
}

export function geminiModelOptionsFromEffortValue(
  value: string | null | undefined,
): GeminiModelOptions | undefined {
  const trimmed = trimOrNull(value);
  if (!trimmed) return undefined;
  if (isGeminiThinkingLevel(trimmed)) return { thinkingLevel: trimmed };
  if (isGeminiThinkingBudget(trimmed)) {
    return { thinkingBudget: GEMINI_THINKING_BUDGET_MAP.get(trimmed) };
  }
  return undefined;
}

export function getGeminiThinkingModelAlias(
  model: string,
  modelOptions: GeminiModelOptions | null | undefined,
): string | null {
  const kind = getGeminiThinkingConfigKind(model);
  if (!kind || !modelOptions) return null;

  const capabilities = geminiCapabilitiesForModel(model);
  const effort =
    modelOptions.thinkingLevel ??
    (modelOptions.thinkingBudget === undefined ? undefined : String(modelOptions.thinkingBudget));
  if (!effort || !hasEffortLevel(capabilities, effort)) return null;

  const base = sanitizeGeminiAliasSegment(model);
  if (kind === "level" && modelOptions.thinkingLevel) {
    return `synara-gemini-${base}-thinking-level-${modelOptions.thinkingLevel.toLowerCase()}`;
  }
  if (kind === "budget" && modelOptions.thinkingBudget !== undefined) {
    const budget = modelOptions.thinkingBudget === -1 ? "dynamic" : String(modelOptions.thinkingBudget);
    return `synara-gemini-${base}-thinking-budget-${budget}`;
  }
  return null;
}

export function resolveGeminiApiModelId(
  model: string,
  modelOptions: GeminiModelOptions | null | undefined,
): string {
  return getGeminiThinkingModelAlias(model, modelOptions) ?? model;
}
