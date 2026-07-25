// FILE: composerDraftModels.ts
// Purpose: Normalizes provider-scoped model selections and resolves effective composer models.
// Exports: Model state helpers used by persistence, actions, and the public facade.

import {
  GROK_REASONING_EFFORT_OPTIONS,
  ProviderKind,
  type ClaudeCodeEffort,
  type CodexReasoningEffort,
  type CursorModelOptions,
  type DroidReasoningEffort,
  type GrokReasoningEffort,
  type ModelSelection,
  type ModelSlug,
  type PiThinkingLevel,
  type ProviderInstanceId,
  type ProviderModelOptions,
} from "@synara/contracts";
import * as Schema from "effect/Schema";

import {
  getDefaultModel,
  normalizeModelSlug,
  resolveModelSlugForProvider,
  resolveSelectableModel,
} from "@synara/shared/model";
import {
  inferLegacyProviderKindFromInstanceId,
  inferLegacyProviderKindFromModel,
  inferLegacyProviderKindFromModelSelection,
} from "@synara/shared/providerInstances";
import { resolveAppModelSelection } from "./appSettings";
import type {
  ComposerThreadDraftState,
  ModelSelectionByProviderInstance,
} from "./composerDraftDomain";
import { classifyProviderReasoningEffortSupport } from "./lib/codexReasoningEffort";
import { buildModelSelection, buildProviderOptionPatch } from "./providerModelOptions";

export const COMPOSER_PROVIDER_KINDS = [
  "codex",
  "claudeAgent",
  "cursor",
  "antigravity",
  "grok",
  "droid",
  "kilo",
  "opencode",
  "pi",
] as const satisfies readonly ProviderKind[];

const isProviderKind = Schema.is(ProviderKind);

const GROK_REASONING_EFFORT_SET = new Set<string>(GROK_REASONING_EFFORT_OPTIONS);

export const LegacyCodexFields = Schema.Struct({
  effort: Schema.optionalKey(Schema.String),
  codexFastMode: Schema.optionalKey(Schema.Boolean),
  serviceTier: Schema.optionalKey(Schema.String),
});

export type LegacyCodexFields = typeof LegacyCodexFields.Type;

const ANTIGRAVITY_REASONING_EFFORT_SET = new Set(["low", "medium", "high", "thinking"]);

export interface EffectiveComposerModelState {
  selectedModel: ModelSlug;
  modelOptions: ProviderModelOptions | null;
}

function mergeProviderModelOptionsFromSelections(
  ...selections: ReadonlyArray<ModelSelection | null | undefined>
): ProviderModelOptions | null {
  const result: Partial<Record<ProviderKind, ProviderModelOptions[ProviderKind]>> = {};
  for (const selection of selections) {
    if (!selection) continue;
    const provider = inferLegacyProviderKindFromModelSelection(selection);
    const options = providerModelOptionsFromSelection(selection, provider);
    if (options) {
      result[provider] = options;
    } else {
      delete result[provider];
    }
  }
  return Object.keys(result).length > 0 ? (result as ProviderModelOptions) : null;
}

function modelSelectionMatchesProviderInstance(
  selection: ModelSelection | null | undefined,
  provider: ProviderKind,
  instanceId: ProviderInstanceId | null | undefined,
): selection is ModelSelection {
  if (!selection) return false;
  return selection.instanceId === (instanceId ?? provider);
}

function providerModelOptionsFromSelection(
  selection: ModelSelection,
  provider: ProviderKind,
): ProviderModelOptions[ProviderKind] | undefined {
  if (!selection.options || selection.options.length === 0) return undefined;
  const optionPatch = selection.options.reduce<Record<string, unknown>>((acc, option) => {
    Object.assign(acc, buildProviderOptionPatch(provider, option.id, option.value));
    return acc;
  }, {});
  return normalizeProviderModelOptions({ [provider]: optionPatch }, provider)?.[provider];
}

function deriveEffectiveComposerModelOptions(input: {
  draft:
    | Pick<ComposerThreadDraftState, "modelSelectionByProvider" | "activeProvider">
    | null
    | undefined;
  selectedProvider: ProviderKind;
  selectedProviderInstanceId?: ProviderInstanceId | null | undefined;
  selectedProviderInstanceId?: ProviderInstanceId | null | undefined;
  threadModelSelection: ModelSelection | null | undefined;
  projectModelSelection: ModelSelection | null | undefined;
}): ProviderModelOptions | null {
  const selectedOnly = (selection: ModelSelection | null | undefined) =>
    selection &&
    inferLegacyProviderKindFromModelSelection(selection) === input.selectedProvider &&
    !modelSelectionMatchesProviderInstance(
      selection,
      input.selectedProvider,
      input.selectedProviderInstanceId,
    )
      ? null
      : selection;
  const baseOptions = mergeProviderModelOptionsFromSelections(
    selectedOnly(input.projectModelSelection),
    selectedOnly(input.threadModelSelection),
  );
  const draftSelections = input.draft?.modelSelectionByProvider;
  if (!draftSelections) {
    return baseOptions;
  }

  const result: Partial<Record<ProviderKind, ProviderModelOptions[ProviderKind]>> = baseOptions
    ? { ...baseOptions }
    : {};
  for (const selection of Object.values(draftSelections)) {
    if (!selection) continue;
    const provider = inferLegacyProviderKindFromModelSelection(selection);
    if (
      provider === input.selectedProvider &&
      !modelSelectionMatchesProviderInstance(
        selection,
        input.selectedProvider,
        input.selectedProviderInstanceId,
      )
    ) {
      continue;
    }
    const options = providerModelOptionsFromSelection(selection, provider);
    if (options) {
      result[provider] = options;
    } else {
      delete result[provider];
    }
  }
  return Object.keys(result).length > 0 ? (result as ProviderModelOptions) : null;
}

export function normalizeProviderKind(value: unknown): ProviderKind | null {
  if (value === "gemini") {
    return "antigravity";
  }
  return isProviderKind(value) ? value : null;
}

function trimStringOrUndefined(value: unknown): string | undefined {
  if (typeof value !== "string") {
    return undefined;
  }
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

function normalizeProviderInstanceId(value: unknown): ProviderInstanceId | undefined {
  const trimmed = trimStringOrUndefined(value);
  return trimmed === undefined ? undefined : (trimmed as ProviderInstanceId);
}

export function normalizeActiveProviderInstanceId(value: unknown): ProviderInstanceId | null {
  return normalizeProviderInstanceId(value) ?? null;
}

export function providerInstanceModelSelectionKey(
  provider: ProviderKind,
  instanceId?: ProviderInstanceId | null | undefined,
): ProviderInstanceId {
  return (normalizeProviderInstanceId(instanceId) ?? provider) as ProviderInstanceId;
}

export function modelSelectionStorageKey(selection: ModelSelection): ProviderInstanceId {
  return (normalizeProviderInstanceId(selection.instanceId) ??
    inferLegacyProviderKindFromModelSelection(selection)) as ProviderInstanceId;
}

export function readModelSelectionForProviderInstance(
  selections: ModelSelectionByProviderInstance | null | undefined,
  provider: ProviderKind,
  instanceId?: ProviderInstanceId | null | undefined,
): ModelSelection | undefined {
  return selections?.[providerInstanceModelSelectionKey(provider, instanceId)];
}

export function normalizeModelSelectionMapByInstance(
  selections: ModelSelectionByProviderInstance,
): ModelSelectionByProviderInstance {
  const result: ModelSelectionByProviderInstance = {};
  for (const [key, selection] of Object.entries(selections)) {
    const normalized = normalizeModelSelection(selection, { provider: key });
    if (normalized) result[modelSelectionStorageKey(normalized)] = normalized;
  }
  return result;
}

function isGrokReasoningEffort(value: unknown): value is GrokReasoningEffort {
  return typeof value === "string" && GROK_REASONING_EFFORT_SET.has(value);
}

export function makeModelSelection(
  provider: ProviderKind,
  model: string,
  options?: ProviderModelOptions[ProviderKind],
  instanceId?: ProviderInstanceId | null | undefined,
): ModelSelection {
  return buildModelSelection(provider, model, options, { instanceId });
}

export function normalizeProviderModelOptions(
  value: unknown,
  provider?: ProviderKind | null,
  legacy?: LegacyCodexFields,
): ProviderModelOptions | null {
  const candidate = value && typeof value === "object" ? (value as Record<string, unknown>) : null;
  const codexCandidate =
    candidate?.codex && typeof candidate.codex === "object"
      ? (candidate.codex as Record<string, unknown>)
      : null;
  const claudeCandidate =
    candidate?.claudeAgent && typeof candidate.claudeAgent === "object"
      ? (candidate.claudeAgent as Record<string, unknown>)
      : null;
  const cursorCandidate =
    candidate?.cursor && typeof candidate.cursor === "object"
      ? (candidate.cursor as Record<string, unknown>)
      : null;
  const antigravityCandidate =
    candidate?.antigravity && typeof candidate.antigravity === "object"
      ? (candidate.antigravity as Record<string, unknown>)
      : null;
  const grokCandidate =
    candidate?.grok && typeof candidate.grok === "object"
      ? (candidate.grok as Record<string, unknown>)
      : null;
  const droidCandidate =
    candidate?.droid && typeof candidate.droid === "object"
      ? (candidate.droid as Record<string, unknown>)
      : null;
  const openCodeCandidate =
    candidate?.opencode && typeof candidate.opencode === "object"
      ? (candidate.opencode as Record<string, unknown>)
      : null;
  const kiloCandidate =
    candidate?.kilo && typeof candidate.kilo === "object"
      ? (candidate.kilo as Record<string, unknown>)
      : null;
  const piCandidate =
    candidate?.pi && typeof candidate.pi === "object"
      ? (candidate.pi as Record<string, unknown>)
      : null;

  const codexReasoningEffort: CodexReasoningEffort | undefined =
    trimStringOrUndefined(codexCandidate?.reasoningEffort) ??
    (provider === "codex" ? trimStringOrUndefined(legacy?.effort) : undefined);
  const codexFastMode =
    codexCandidate?.fastMode === true
      ? true
      : codexCandidate?.fastMode === false
        ? false
        : (provider === "codex" && legacy?.codexFastMode === true) ||
            (typeof legacy?.serviceTier === "string" && legacy.serviceTier === "fast")
          ? true
          : undefined;
  const codex =
    codexReasoningEffort !== undefined || codexFastMode !== undefined
      ? {
          ...(codexReasoningEffort !== undefined ? { reasoningEffort: codexReasoningEffort } : {}),
          ...(codexFastMode !== undefined ? { fastMode: codexFastMode } : {}),
        }
      : undefined;

  const claudeThinking =
    claudeCandidate?.thinking === true
      ? true
      : claudeCandidate?.thinking === false
        ? false
        : undefined;
  const claudeEffort: ClaudeCodeEffort | undefined =
    claudeCandidate?.effort === "low" ||
    claudeCandidate?.effort === "medium" ||
    claudeCandidate?.effort === "high" ||
    claudeCandidate?.effort === "xhigh" ||
    claudeCandidate?.effort === "max" ||
    claudeCandidate?.effort === "ultrathink" ||
    claudeCandidate?.effort === "ultracode"
      ? claudeCandidate.effort
      : undefined;
  const claudeFastMode =
    claudeCandidate?.fastMode === true
      ? true
      : claudeCandidate?.fastMode === false
        ? false
        : undefined;
  const claudeAutoCompactWindow =
    trimStringOrUndefined(claudeCandidate?.autoCompactWindow) ??
    trimStringOrUndefined(claudeCandidate?.contextWindow);
  const claude =
    claudeThinking !== undefined ||
    claudeEffort !== undefined ||
    claudeFastMode !== undefined ||
    claudeAutoCompactWindow !== undefined
      ? {
          ...(claudeThinking !== undefined ? { thinking: claudeThinking } : {}),
          ...(claudeEffort !== undefined ? { effort: claudeEffort } : {}),
          ...(claudeFastMode !== undefined ? { fastMode: claudeFastMode } : {}),
          ...(claudeAutoCompactWindow !== undefined
            ? { autoCompactWindow: claudeAutoCompactWindow }
            : {}),
        }
      : undefined;

  const cursorReasoningEffort = trimStringOrUndefined(cursorCandidate?.reasoningEffort);
  const cursorFastMode =
    cursorCandidate?.fastMode === true
      ? true
      : cursorCandidate?.fastMode === false
        ? false
        : undefined;
  const cursorThinking =
    cursorCandidate?.thinking === true
      ? true
      : cursorCandidate?.thinking === false
        ? false
        : undefined;
  const cursorContextWindow = trimStringOrUndefined(cursorCandidate?.contextWindow);
  const cursor: CursorModelOptions | undefined =
    cursorReasoningEffort !== undefined ||
    cursorFastMode !== undefined ||
    cursorThinking !== undefined ||
    cursorContextWindow !== undefined
      ? {
          ...(cursorReasoningEffort !== undefined
            ? { reasoningEffort: cursorReasoningEffort }
            : {}),
          ...(cursorFastMode !== undefined ? { fastMode: cursorFastMode } : {}),
          ...(cursorThinking !== undefined ? { thinking: cursorThinking } : {}),
          ...(cursorContextWindow !== undefined ? { contextWindow: cursorContextWindow } : {}),
        }
      : undefined;

  const antigravityReasoningEffort = trimStringOrUndefined(antigravityCandidate?.reasoningEffort);
  const antigravity =
    antigravityReasoningEffort !== undefined
      ? { reasoningEffort: antigravityReasoningEffort }
      : undefined;
  const grokReasoningEffort: GrokReasoningEffort | undefined = isGrokReasoningEffort(
    grokCandidate?.reasoningEffort,
  )
    ? grokCandidate.reasoningEffort
    : undefined;
  const grok =
    grokReasoningEffort !== undefined ? { reasoningEffort: grokReasoningEffort } : undefined;
  const droidReasoningEffort: DroidReasoningEffort | undefined = trimStringOrUndefined(
    droidCandidate?.reasoningEffort,
  );
  const droid =
    droidReasoningEffort !== undefined ? { reasoningEffort: droidReasoningEffort } : undefined;
  const openCodeVariant = trimStringOrUndefined(openCodeCandidate?.variant);
  const openCodeAgent = trimStringOrUndefined(openCodeCandidate?.agent);
  const opencode =
    openCodeVariant !== undefined || openCodeAgent !== undefined
      ? {
          ...(openCodeVariant !== undefined ? { variant: openCodeVariant } : {}),
          ...(openCodeAgent !== undefined ? { agent: openCodeAgent } : {}),
        }
      : undefined;
  const kiloVariant = trimStringOrUndefined(kiloCandidate?.variant);
  const kiloAgent = trimStringOrUndefined(kiloCandidate?.agent);
  const kilo =
    kiloVariant !== undefined || kiloAgent !== undefined
      ? {
          ...(kiloVariant !== undefined ? { variant: kiloVariant } : {}),
          ...(kiloAgent !== undefined ? { agent: kiloAgent } : {}),
        }
      : undefined;
  const piThinkingLevel: PiThinkingLevel | undefined =
    piCandidate?.thinkingLevel === "off" ||
    piCandidate?.thinkingLevel === "minimal" ||
    piCandidate?.thinkingLevel === "low" ||
    piCandidate?.thinkingLevel === "medium" ||
    piCandidate?.thinkingLevel === "high" ||
    piCandidate?.thinkingLevel === "xhigh"
      ? piCandidate.thinkingLevel
      : undefined;
  const pi = piThinkingLevel !== undefined ? { thinkingLevel: piThinkingLevel } : undefined;
  if (
    !codex &&
    !claude &&
    !cursor &&
    !antigravity &&
    !grok &&
    !droid &&
    !kilo &&
    !opencode &&
    !pi
  ) {
    return null;
  }
  return {
    ...(codex ? { codex } : {}),
    ...(claude ? { claudeAgent: claude } : {}),
    ...(cursor ? { cursor } : {}),
    ...(antigravity ? { antigravity } : {}),
    ...(grok ? { grok } : {}),
    ...(droid ? { droid } : {}),
    ...(kilo ? { kilo } : {}),
    ...(opencode ? { opencode } : {}),
    ...(pi ? { pi } : {}),
  };
}

export function normalizeModelSelection(
  value: unknown,
  legacy?: {
    provider?: unknown;
    model?: unknown;
    modelOptions?: unknown;
    legacyCodex?: LegacyCodexFields;
  },
): ModelSelection | null {
  const candidate = value && typeof value === "object" ? (value as Record<string, unknown>) : null;
  const rawModel = candidate?.model ?? legacy?.model;
  if (typeof rawModel !== "string") {
    return null;
  }
  const instanceId = normalizeProviderInstanceId(candidate?.instanceId);
  const provider =
    normalizeProviderKind(candidate?.provider ?? legacy?.provider) ??
    inferLegacyProviderKindFromInstanceId(instanceId) ??
    inferLegacyProviderKindFromModel(rawModel);
  const inferredClaudeAutoCompactWindow =
    provider === "claudeAgent" && /\[1m\]$/iu.test(rawModel) ? "1m" : undefined;
  const model = normalizeModelSlug(rawModel, provider);
  if (!model) {
    return null;
  }
  const modelOptions = normalizeProviderModelOptions(
    Array.isArray(candidate?.options)
      ? {
          [provider]: providerModelOptionsFromSelection(
            {
              instanceId: instanceId ?? provider,
              model: rawModel,
              options: candidate.options as ModelSelection["options"],
            },
            provider,
          ),
        }
      : candidate?.options
        ? { [provider]: candidate.options }
        : legacy?.modelOptions,
    provider,
    provider === "codex" ? legacy?.legacyCodex : undefined,
  );
  const options =
    provider === "codex"
      ? modelOptions?.codex
      : provider === "claudeAgent"
        ? inferredClaudeAutoCompactWindow !== undefined
          ? {
              ...modelOptions?.claudeAgent,
              autoCompactWindow:
                modelOptions?.claudeAgent?.autoCompactWindow ?? inferredClaudeAutoCompactWindow,
            }
          : modelOptions?.claudeAgent
        : provider === "antigravity"
          ? modelOptions?.antigravity
          : provider === "grok"
            ? modelOptions?.grok
            : provider === "droid"
              ? modelOptions?.droid
              : provider === "kilo"
                ? modelOptions?.kilo
                : provider === "cursor"
                  ? modelOptions?.cursor
                  : provider === "opencode"
                    ? modelOptions?.opencode
                    : provider === "pi"
                      ? modelOptions?.pi
                      : undefined;
  return makeModelSelection(provider, model, options, instanceId);
}

export function reconcileProviderScopedModelSelection(
  requested: ModelSelection,
  current: ModelSelection | null | undefined,
): ModelSelection {
  const provider = inferLegacyProviderKindFromModelSelection(requested);
  if (
    requested.options !== undefined ||
    !current ||
    inferLegacyProviderKindFromModelSelection(current) !== provider
  ) {
    return requested;
  }
  if (current.model === requested.model) {
    return makeModelSelection(
      provider,
      requested.model,
      providerModelOptionsFromSelection(current, provider),
      requested.instanceId,
    );
  }
  if (provider !== "codex" && provider !== "cursor" && provider !== "claudeAgent") {
    return requested;
  }
  let preservedOptions = providerModelOptionsFromSelection(current, provider);
  const effort = provider === "claudeAgent" ? preservedOptions?.effort : preservedOptions?.reasoningEffort;
  if (
    effort !== undefined &&
    classifyProviderReasoningEffortSupport({
      provider,
      model: requested.model,
      effort,
    }) !== "supported"
  ) {
    const { reasoningEffort: _reasoningEffort, effort: _effort, ...remainingOptions } =
      preservedOptions ?? {};
    preservedOptions =
      Object.keys(remainingOptions).length > 0 ? remainingOptions : undefined;
  }
  return makeModelSelection(provider, requested.model, preservedOptions, requested.instanceId);
}

export function stripNonStickyModelOptions(selection: ModelSelection): ModelSelection {
  if (inferLegacyProviderKindFromModelSelection(selection) !== "claudeAgent") return selection;
  const options = selection.options ?? [];
  const stickyOptions = options.filter(
    (option) => option.id !== "contextWindow" && option.id !== "autoCompactWindow",
  );
  if (stickyOptions.length === options.length) return selection;
  return stickyOptions.length > 0
    ? { ...selection, options: stickyOptions }
    : { instanceId: selection.instanceId, model: selection.model };
}

export function sanitizeStickyModelSelectionMap(
  map: ModelSelectionByProviderInstance,
): ModelSelectionByProviderInstance {
  let sanitized = map;
  for (const [instanceId, selection] of Object.entries(map)) {
    if (!selection) continue;
    const sticky = stripNonStickyModelOptions(selection);
    if (sticky === selection) continue;
    if (sanitized === map) sanitized = { ...map };
    sanitized[instanceId as ProviderInstanceId] = sticky;
  }
  return sanitized;
}

export function legacySyncModelSelectionOptions(
  modelSelection: ModelSelection | null,
  modelOptions: ProviderModelOptions | null | undefined,
): ModelSelection | null {
  if (modelSelection === null) {
    return null;
  }
  const provider = inferLegacyProviderKindFromModelSelection(modelSelection);
  return makeModelSelection(
    provider,
    modelSelection.model,
    modelOptions?.[provider],
    modelSelection.instanceId,
  );
}

export function legacyMergeModelSelectionIntoProviderModelOptions(
  modelSelection: ModelSelection | null,
  currentModelOptions: ProviderModelOptions | null | undefined,
): ProviderModelOptions | null {
  if (modelSelection?.options === undefined) {
    return normalizeProviderModelOptions(currentModelOptions);
  }
  return legacyReplaceProviderModelOptions(
    normalizeProviderModelOptions(currentModelOptions),
    inferLegacyProviderKindFromModelSelection(modelSelection),
    providerModelOptionsFromSelection(
      modelSelection,
      inferLegacyProviderKindFromModelSelection(modelSelection),
    ),
  );
}

function legacyReplaceProviderModelOptions(
  currentModelOptions: ProviderModelOptions | null | undefined,
  provider: ProviderKind,
  nextProviderOptions: ProviderModelOptions[ProviderKind] | null | undefined,
): ProviderModelOptions | null {
  const { [provider]: _discardedProviderModelOptions, ...otherProviderModelOptions } =
    currentModelOptions ?? {};
  const normalizedNextProviderOptions = normalizeProviderModelOptions(
    { [provider]: nextProviderOptions },
    provider,
  );

  return normalizeProviderModelOptions({
    ...otherProviderModelOptions,
    ...(normalizedNextProviderOptions ? normalizedNextProviderOptions : {}),
  });
}

export function legacyToModelSelectionByProvider(
  modelSelection: ModelSelection | null,
  modelOptions: ProviderModelOptions | null | undefined,
): ModelSelectionByProviderInstance {
  const result: ModelSelectionByProviderInstance = {};
  // Add entries from the options bag (for non-active providers)
  if (modelOptions) {
    for (const provider of COMPOSER_PROVIDER_KINDS) {
      const options = modelOptions[provider];
      if (options && Object.keys(options).length > 0) {
        const model =
          modelSelection &&
          inferLegacyProviderKindFromModelSelection(modelSelection) === provider
            ? modelSelection.model
            : getDefaultModel(provider);
        if (model) {
          result[providerInstanceModelSelectionKey(provider)] = makeModelSelection(
            provider,
            model,
            options,
          );
        }
      }
    }
  }
  // Add/overwrite the active selection (it's authoritative for its provider)
  if (modelSelection) {
    result[modelSelectionStorageKey(modelSelection)] = modelSelection;
  }
  return result;
}

export function deriveEffectiveComposerModelState(input: {
  draft:
    | Pick<ComposerThreadDraftState, "modelSelectionByProvider" | "activeProvider">
    | null
    | undefined;
  selectedProvider: ProviderKind;
  threadModelSelection: ModelSelection | null | undefined;
  projectModelSelection: ModelSelection | null | undefined;
  customModelsByProvider: Record<ProviderKind, readonly string[]>;
  availableModelOptionsByProvider?: Partial<
    Record<ProviderKind, ReadonlyArray<{ slug: string; name: string }>>
  >;
}): EffectiveComposerModelState {
  const selectionMatchesSelectedInstance = (
    selection: ModelSelection | null | undefined,
  ): selection is ModelSelection =>
    modelSelectionMatchesProviderInstance(
      selection,
      input.selectedProvider,
      input.selectedProviderInstanceId,
    );
  const resolveAvailableModel = (candidate: string | null | undefined): ModelSlug | null => {
    const availableOptions = input.availableModelOptionsByProvider?.[input.selectedProvider];
    if (!availableOptions || availableOptions.length === 0) {
      return null;
    }
    return resolveSelectableModel(input.selectedProvider, candidate, availableOptions);
  };
  const baseModel = resolveModelSlugForProvider(
    input.selectedProvider,
    (selectionMatchesSelectedInstance(input.threadModelSelection)
      ? input.threadModelSelection.model
      : null) ??
      (selectionMatchesSelectedInstance(input.projectModelSelection)
        ? input.projectModelSelection.model
        : null) ??
      getDefaultModel(input.selectedProvider),
  );
  const persistedThreadModel = selectionMatchesSelectedInstance(input.threadModelSelection)
    ? (normalizeModelSlug(input.threadModelSelection.model, input.selectedProvider) ??
      input.threadModelSelection.model)
    : null;
  const persistedProjectModel = selectionMatchesSelectedInstance(input.projectModelSelection)
    ? (normalizeModelSlug(input.projectModelSelection.model, input.selectedProvider) ??
      input.projectModelSelection.model)
    : null;
  const activeSelection = readModelSelectionForProviderInstance(
    input.draft?.modelSelectionByProvider,
    input.selectedProvider,
    input.selectedProviderInstanceId,
  );
  const selectedDraftModel = activeSelection?.model
    ? resolveAppModelSelection(
        input.selectedProvider,
        input.customModelsByProvider,
        activeSelection.model,
      )
    : null;
  const unlistedDraftModel = input.selectedProvider === "pi" ? selectedDraftModel : null;
  const selectedModel =
    resolveAvailableModel(activeSelection?.model) ??
    resolveAvailableModel(
      selectionMatchesSelectedInstance(input.threadModelSelection)
        ? input.threadModelSelection.model
        : null,
    ) ??
    resolveAvailableModel(
      selectionMatchesSelectedInstance(input.projectModelSelection)
        ? input.projectModelSelection.model
        : null,
    ) ??
    resolveAvailableModel(selectedDraftModel) ??
    persistedThreadModel ??
    persistedProjectModel ??
    unlistedDraftModel ??
    input.availableModelOptionsByProvider?.[input.selectedProvider]?.[0]?.slug ??
    selectedDraftModel ??
    baseModel ??
    getDefaultModel("codex");
  const modelOptions = deriveEffectiveComposerModelOptions(input);

  return {
    selectedModel,
    modelOptions,
  };
}

export function resolvePreferredComposerModelSelection(input: {
  draft:
    | Pick<ComposerThreadDraftState, "modelSelectionByProvider" | "activeProvider">
    | null
    | undefined;
  threadModelSelection: ModelSelection | null | undefined;
  projectModelSelection: ModelSelection | null | undefined;
  defaultProvider?: ProviderKind | null | undefined;
  resolveProviderForInstanceId?: (
    instanceId: ProviderInstanceId,
  ) => ProviderKind | null | undefined;
}): ModelSelection {
  const activeInstanceId = input.draft?.activeProvider ?? null;
  const resolveSelectionProvider = (
    selection: Pick<ModelSelection, "instanceId" | "model"> | null | undefined,
  ): ProviderKind | null =>
    selection
      ? (input.resolveProviderForInstanceId?.(selection.instanceId) ??
        inferLegacyProviderKindFromModelSelection(selection))
      : null;
  const activeInstanceProvider = activeInstanceId
    ? (input.resolveProviderForInstanceId?.(activeInstanceId) ??
      inferLegacyProviderKindFromInstanceId(activeInstanceId) ??
      null)
    : null;
  const activeDraftSelection = activeInstanceId
    ? input.draft?.modelSelectionByProvider[activeInstanceId]
    : undefined;
  const draftProviderWithSelection = activeDraftSelection
    ? resolveSelectionProvider(activeDraftSelection)
    : null;
  const preferredProvider =
    draftProviderWithSelection ??
    activeInstanceProvider ??
    resolveSelectionProvider(input.threadModelSelection) ??
    resolveSelectionProvider(input.projectModelSelection) ??
    input.defaultProvider ??
    "codex";
  const fallbackInstanceId = (
    activeInstanceId && activeInstanceProvider === preferredProvider
      ? activeInstanceId
      : preferredProvider === "pi"
        ? "codex"
        : preferredProvider
  ) as ProviderInstanceId;

  return (
    readModelSelectionForProviderInstance(
      input.draft?.modelSelectionByProvider,
      preferredProvider,
      activeDraftSelection?.instanceId ?? activeInstanceId,
    ) ??
    (input.threadModelSelection &&
    resolveSelectionProvider(input.threadModelSelection) === preferredProvider
      ? input.threadModelSelection
      : null) ??
    (input.projectModelSelection &&
    resolveSelectionProvider(input.projectModelSelection) === preferredProvider
      ? input.projectModelSelection
      : null) ?? {
      instanceId: fallbackInstanceId,
      model: getDefaultModel(preferredProvider === "pi" ? "codex" : preferredProvider),
    }
  );
}
