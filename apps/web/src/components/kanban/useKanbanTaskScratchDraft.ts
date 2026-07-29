// FILE: useKanbanTaskScratchDraft.ts
// Purpose: Owns the throwaway composer-draft thread used by the kanban new-task dialog.
// Layer: Kanban UI hook
// Exports: useKanbanTaskScratchDraft

import type { ModelSlug, ProviderInstanceId, ProviderKind } from "@synara/contracts";
import { getDefaultModel } from "@synara/shared/model";
import {
  inferLegacyProviderKindFromInstanceId,
  inferLegacyProviderKindFromModelSelection,
} from "@synara/shared/providerInstances";
import { useEffect, useMemo, useRef, useState } from "react";

import {
  filterPromptProviderMentionReferences,
  filterPromptSkillReferences,
  providerMentionReferencesEqual,
  providerSkillReferencesEqual,
} from "~/lib/composerMentions";
import { buildComposerImageAttachmentsFromFiles } from "~/lib/composerSend";
import { newThreadId } from "~/lib/utils";
import {
  getProviderInstanceOptions,
  resolveSelectableProviderInstanceId,
  type AppSettings,
} from "../../appSettings";
import {
  providerInstanceModelSelectionKey,
  useComposerDraftStore,
  useComposerThreadDraft,
} from "../../composerDraftStore";
import { buildModelSelection, providerOptionsFromSelections } from "../../providerModelOptions";
import { toastManager } from "../ui/toast";

export function useKanbanTaskScratchDraft(input: {
  readonly defaultProvider: ProviderKind;
  readonly settings: Pick<
    AppSettings,
    "codexAccounts" | "codexHomePath" | "providerInstances" | "selectedCodexAccountId"
  >;
}) {
  // Scratch composer draft backing the dialog: model/effort/speed state lives in
  // the composer draft store under this throwaway thread id, exactly like chat.
  const [scratchThreadId] = useState(() => newThreadId());
  useEffect(() => {
    useComposerDraftStore.getState().applyStickyState(scratchThreadId);
    return () => {
      useComposerDraftStore.getState().clearDraftThread(scratchThreadId);
    };
  }, [scratchThreadId]);

  const scratchDraft = useComposerThreadDraft(scratchThreadId);
  const prompt = scratchDraft.prompt;
  const composerImages = scratchDraft.images;
  const composerAssistantSelections = scratchDraft.assistantSelections;
  const composerFileComments = scratchDraft.fileComments;
  const composerTerminalContexts = scratchDraft.terminalContexts;
  const composerSkills = scratchDraft.skills;
  const composerMentions = scratchDraft.mentions;
  const nonPersistedComposerImageIdSet = new Set(scratchDraft.nonPersistedImageIds);

  const setPrompt = (nextPrompt: string) => {
    useComposerDraftStore.getState().setPrompt(scratchThreadId, nextPrompt);
  };

  const stickyActiveProvider = useComposerDraftStore((state) => state.stickyActiveProvider);
  const stickyModelSelectionByProvider = useComposerDraftStore(
    (state) => state.stickyModelSelectionByProvider,
  );
  const activeProviderInstanceId = scratchDraft.activeProvider ?? stickyActiveProvider;
  const providerInstances = useMemo(
    () => getProviderInstanceOptions(input.settings),
    [input.settings],
  );
  const selectedProvider: ProviderKind =
    (activeProviderInstanceId
      ? (providerInstances.find((instance) => instance.instanceId === activeProviderInstanceId)
          ?.provider ?? inferLegacyProviderKindFromInstanceId(activeProviderInstanceId))
      : null) ?? input.defaultProvider;
  const selectedProviderInstanceId: ProviderInstanceId = resolveSelectableProviderInstanceId(
    input.settings,
    selectedProvider,
    activeProviderInstanceId ??
      Object.values(scratchDraft.modelSelectionByProvider).find(
        (selection) =>
          selection !== undefined &&
          inferLegacyProviderKindFromModelSelection(selection) === selectedProvider,
      )?.instanceId ??
      Object.values(stickyModelSelectionByProvider).find(
        (selection) =>
          selection !== undefined &&
          inferLegacyProviderKindFromModelSelection(selection) === selectedProvider,
      )?.instanceId,
  );
  const selectionKey = providerInstanceModelSelectionKey(
    selectedProvider,
    selectedProviderInstanceId,
  );
  const draftModelSelection =
    scratchDraft.modelSelectionByProvider[selectionKey] ??
    stickyModelSelectionByProvider[selectionKey];
  const selectedModel: ModelSlug | null =
    draftModelSelection?.model ?? getDefaultModel(selectedProvider);
  const selectedProviderModelOptions = providerOptionsFromSelections(
    selectedProvider,
    draftModelSelection?.options,
  );
  const selectedModelSupportsAutoMode =
    selectedProvider === "claudeAgent"
      ? draftModelSelection?.supportsAutoMode
      : undefined;

  const previousSelectedProviderRef = useRef<{
    threadId: string;
    provider: ProviderKind;
  } | null>(null);

  useEffect(() => {
    const nextSkills = filterPromptSkillReferences(prompt, composerSkills, selectedProvider);
    if (!providerSkillReferencesEqual(composerSkills, nextSkills)) {
      useComposerDraftStore.getState().setSkills(scratchThreadId, nextSkills);
    }
  }, [composerSkills, prompt, scratchThreadId, selectedProvider]);

  useEffect(() => {
    const nextMentions = filterPromptProviderMentionReferences(prompt, composerMentions);
    if (!providerMentionReferencesEqual(composerMentions, nextMentions)) {
      useComposerDraftStore.getState().setMentions(scratchThreadId, nextMentions);
    }
  }, [composerMentions, prompt, scratchThreadId]);

  useEffect(() => {
    const previous = previousSelectedProviderRef.current;
    previousSelectedProviderRef.current = {
      threadId: scratchThreadId,
      provider: selectedProvider,
    };
    if (
      !previous ||
      previous.threadId !== scratchThreadId ||
      previous.provider === selectedProvider
    ) {
      return;
    }
    useComposerDraftStore.getState().setSkills(scratchThreadId, []);
    useComposerDraftStore.getState().setMentions(scratchThreadId, []);
  }, [scratchThreadId, selectedProvider]);

  const handleProviderModelChange = (
    provider: ProviderKind,
    model: ModelSlug,
    instanceId?: ProviderInstanceId,
    supportsAutoMode?: boolean,
  ) => {
    const store = useComposerDraftStore.getState();
    const nextSelection = buildModelSelection(provider, model, undefined, {
      instanceId: instanceId ?? provider,
      supportsAutoMode: provider === "claudeAgent" ? supportsAutoMode : undefined,
    });
    // Mirrors the composer: update the scratch draft and persist the sticky selection.
    store.setModelSelectionAndSticky(scratchThreadId, nextSelection);
  };

  const addComposerImages = (files: readonly File[]) => {
    if (files.length === 0) return;
    const { images, error } = buildComposerImageAttachmentsFromFiles({
      files,
      existingAttachmentCount: composerImages.length + composerAssistantSelections.length,
    });
    if (images.length > 0) {
      useComposerDraftStore.getState().addImages(scratchThreadId, images);
    }
    if (error) {
      toastManager.add({ type: "warning", title: error });
    }
  };

  const removeComposerImage = (imageId: string) => {
    useComposerDraftStore.getState().removeImage(scratchThreadId, imageId);
  };

  const clearComposerAssistantSelections = () => {
    useComposerDraftStore.getState().clearAssistantSelections(scratchThreadId);
  };

  const clearComposerFileComments = () => {
    useComposerDraftStore.getState().clearFileComments(scratchThreadId);
  };

  const removeComposerTerminalContext = (contextId: string) => {
    useComposerDraftStore.getState().removeTerminalContext(scratchThreadId, contextId);
  };

  return {
    scratchThreadId,
    scratchDraft,
    prompt,
    composerImages,
    composerAssistantSelections,
    composerFileComments,
    composerTerminalContexts,
    composerSkills,
    composerMentions,
    nonPersistedComposerImageIdSet,
      selectedProvider,
      selectedModel,
      selectedProviderInstanceId,
      selectedModelSupportsAutoMode,
    selectedProviderModelOptions,
    setPrompt,
    handleProviderModelChange,
    addComposerImages,
    removeComposerImage,
    clearComposerAssistantSelections,
    clearComposerFileComments,
    removeComposerTerminalContext,
  };
}
