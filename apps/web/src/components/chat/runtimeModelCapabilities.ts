// FILE: runtimeModelCapabilities.ts
// Purpose: Bridges runtime-discovered model metadata into composer capabilities without replacing static defaults wholesale.
// Layer: Chat composer helpers
// Exports: runtime model lookup and Codex capability overrides derived from provider discovery responses.

import type {
  EffortOption,
  ModelCapabilities,
  ProviderKind,
  ProviderModelDescriptor,
} from "@synara/contracts";
import {
  getDefaultEffort,
  getModelCapabilities,
  normalizeModelSlug,
  trimOrNull,
} from "@synara/shared/model";
import { normalizeCursorModelVariantBaseId } from "../../cursorModelVariants";

const BUILT_IN_GPT_5_6_MODELS = new Set(["gpt-5.6-sol", "gpt-5.6-terra", "gpt-5.6-luna"]);

function runtimeEffortLabel(value: string): string {
  switch (value) {
    case "none":
      return "None";
    case "minimal":
      return "Minimal";
    case "low":
      return "Low";
    case "medium":
      return "Medium";
    case "high":
      return "High";
    case "xhigh":
      return "Extra High";
    case "max":
      return "Max";
    case "ultra":
      return "Ultra";
    default:
      return value
        .split(/[-_\s]+/u)
        .filter((segment) => segment.length > 0)
        .map((segment) => segment.charAt(0).toUpperCase() + segment.slice(1))
        .join(" ");
  }
}

// Matches the selected model to its runtime descriptor after provider-specific normalization.
export function resolveRuntimeModelDescriptor(input: {
  provider: ProviderKind;
  model: string | null | undefined;
  runtimeModels: ReadonlyArray<ProviderModelDescriptor> | null | undefined;
}): ProviderModelDescriptor | undefined {
  const { provider, model, runtimeModels } = input;
  if (!runtimeModels?.length) {
    return undefined;
  }

  const normalizedModel = normalizeModelSlug(model, provider) ?? trimOrNull(model);
  if (!normalizedModel) {
    return undefined;
  }

  return runtimeModels.find((candidate) => {
    const normalizedCandidate = normalizeModelSlug(candidate.slug, provider) ?? candidate.slug;
    if (normalizedCandidate === normalizedModel) {
      return true;
    }
    return (
      provider === "cursor" &&
      normalizeCursorModelVariantBaseId(normalizedCandidate) ===
        normalizeCursorModelVariantBaseId(normalizedModel)
    );
  });
}

// Reuses static capability flags but lets runtime-discovered models override exposed effort menus.
export function getRuntimeAwareModelCapabilities(input: {
  provider: ProviderKind;
  model: string | null | undefined;
  runtimeModel?: ProviderModelDescriptor | undefined;
}): ModelCapabilities {
  const staticCapabilities = getModelCapabilities(input.provider, input.model);
  // Runtime discovery is authoritative when available; the static table is only a startup fallback.
  const supportsFastMode =
    (input.provider === "codex" || input.provider === "cursor") && input.runtimeModel
      ? input.runtimeModel.supportsFastMode === true
      : staticCapabilities.supportsFastMode;
  const supportsThinkingToggle =
    input.runtimeModel?.supportsThinkingToggle ?? staticCapabilities.supportsThinkingToggle;
  const contextWindowOptions =
    input.runtimeModel?.contextWindowOptions?.map((option) => ({
      value: option.value,
      label: option.label,
      ...(option.isDefault === true ? { isDefault: true as const } : {}),
    })) ?? staticCapabilities.contextWindowOptions;
  const optionDescriptors =
    input.runtimeModel?.optionDescriptors ?? staticCapabilities.optionDescriptors;
  const runtimeEfforts = input.runtimeModel?.supportedReasoningEfforts;
  if (
    (input.provider !== "codex" &&
      input.provider !== "cursor" &&
      input.provider !== "grok" &&
      input.provider !== "kilo" &&
      input.provider !== "opencode" &&
      input.provider !== "pi") ||
    !runtimeEfforts ||
    runtimeEfforts.length === 0
  ) {
    return {
      ...staticCapabilities,
      ...(optionDescriptors ? { optionDescriptors } : {}),
      supportsFastMode,
      supportsThinkingToggle,
      contextWindowOptions,
    };
  }

  const staticDefaultEffort = getDefaultEffort(staticCapabilities);
  const runtimeDefaultEffort =
    trimOrNull(input.runtimeModel?.defaultReasoningEffort) ??
    (staticDefaultEffort && runtimeEfforts.some((effort) => effort.value === staticDefaultEffort)
      ? staticDefaultEffort
      : null);

  const runtimeOptions: EffortOption[] = runtimeEfforts.map((effort) => {
    const description = trimOrNull(effort.description);
    return {
      value: effort.value,
      label: trimOrNull(effort.label) ?? runtimeEffortLabel(effort.value),
      ...(description ? { description } : {}),
      ...(effort.value === runtimeDefaultEffort ? { isDefault: true as const } : {}),
    };
  });

  // GPT-5.6 launched ahead of the app-server model metadata shipped with some
  // Codex builds. Keep the built-in capability contract authoritative for this
  // family so stale discovery cannot remove Max/Ultra (or leak Sol-only Ultra
  // onto Terra/Luna), while still accepting runtime labels and descriptions.
  const normalizedModel = normalizeModelSlug(input.model, input.provider) ?? trimOrNull(input.model);
  const resolvedRuntimeOptions =
    input.provider === "codex" && normalizedModel && BUILT_IN_GPT_5_6_MODELS.has(normalizedModel)
      ? staticCapabilities.reasoningEffortLevels.map((staticOption) => {
          const runtimeOption = runtimeOptions.find((option) => option.value === staticOption.value);
          return runtimeOption ? { ...staticOption, ...runtimeOption } : staticOption;
        })
      : runtimeOptions;

  if (input.provider === "kilo" || input.provider === "opencode") {
    return {
      ...staticCapabilities,
      ...(optionDescriptors ? { optionDescriptors } : {}),
      variantOptions: resolvedRuntimeOptions,
      supportsThinkingToggle,
      contextWindowOptions,
    };
  }

  return {
    ...staticCapabilities,
    ...(optionDescriptors ? { optionDescriptors } : {}),
    supportsFastMode,
    supportsThinkingToggle,
    contextWindowOptions,
    reasoningEffortLevels: resolvedRuntimeOptions,
  };
}
