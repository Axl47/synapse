import { ComposerDraftImportId } from "@synara/contracts";
import { useNavigate } from "@tanstack/react-router";
import { Schema } from "effect";
import { useEffect, useRef } from "react";

import {
  ExternalComposerDraftImportError,
  hydrateExternalComposerDraftImport,
} from "../externalComposerDraftImport";
import { ensureNativeApi } from "../nativeApi";
import { useStore } from "../store";
import { toastManager } from "./ui/toast";

export function ExternalComposerDraftImportCoordinator() {
  const navigate = useNavigate();
  const pending = useRef(Promise.resolve());
  const queuedOrActiveImportIds = useRef(new Set<string>());

  useEffect(() => {
    const bridge = window.desktopBridge?.composerDraftImports;
    if (!bridge) return;
    return bridge.onIntent((rawImportId) => {
      if (!Schema.is(ComposerDraftImportId)(rawImportId)) return;
      if (queuedOrActiveImportIds.current.has(rawImportId)) return;
      queuedOrActiveImportIds.current.add(rawImportId);
      pending.current = pending.current.then(async () => {
        const loadingToast = toastManager.add({
          type: "loading",
          title: "Opening imported draft",
          description: "Saving the prompt and attachments locally…",
          data: { allowCrossThreadVisibility: true },
        });
        try {
          const result = await hydrateExternalComposerDraftImport({
            importId: rawImportId,
            api: ensureNativeApi(),
            projectWorkspaceRoot: (projectId) =>
              useStore.getState().projects.find((project) => project.id === projectId)?.cwd ??
              null,
          });
          toastManager.close(loadingToast);
          await navigate({
            to: "/$threadId",
            params: { threadId: result.threadId },
          });
          toastManager.add({
            type: "success",
            title: "Draft imported",
            description: "Review it in the composer before sending.",
            data: {
              allowCrossThreadVisibility: true,
              threadId: result.threadId,
            },
          });
        } catch (cause) {
          toastManager.close(loadingToast);
          toastManager.add({
            type: "error",
            title: "Could not import draft",
            description:
              cause instanceof ExternalComposerDraftImportError || cause instanceof Error
                ? cause.message
                : "The imported draft can be retried from Pragma.",
            data: { allowCrossThreadVisibility: true },
          });
        } finally {
          queuedOrActiveImportIds.current.delete(rawImportId);
        }
      });
    });
  }, [navigate]);

  return null;
}
