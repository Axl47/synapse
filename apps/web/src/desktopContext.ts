// FILE: desktopContext.ts
// Purpose: Publishes the route-focused project/thread to the server for desktop integrations.
// Layer: Web route integration

import type { ServerSetDesktopContextInput } from "@t3tools/contracts";
import { useEffect, useMemo, useRef } from "react";

import type { FocusedChatContext } from "./focusedChatContext";
import { useFocusedChatContext } from "./focusedChatContext";
import { readNativeApi } from "./nativeApi";

export function resolveDesktopContextInput(
  context: FocusedChatContext,
): Omit<ServerSetDesktopContextInput, "updatedAt"> {
  const activeProject = context.activeProject;
  if (!activeProject) {
    return {
      projectId: null,
      projectTitle: null,
      workspaceRoot: null,
      threadId: null,
      threadTitle: null,
    };
  }

  const activeThreadId =
    context.activeThread || context.activeDraftThread ? context.focusedThreadId : null;

  return {
    projectId: activeProject.id,
    projectTitle: activeProject.remoteName || activeProject.name,
    workspaceRoot: activeProject.cwd,
    threadId: activeThreadId,
    threadTitle: context.activeThread?.title ?? null,
  };
}

export function usePublishDesktopContext(): void {
  const focusedContext = useFocusedChatContext();
  const lastPublishedKeyRef = useRef<string | null>(null);
  const desktopContext = useMemo(
    () => resolveDesktopContextInput(focusedContext),
    [
      focusedContext.activeDraftThread,
      focusedContext.activeProject,
      focusedContext.activeThread,
      focusedContext.focusedThreadId,
    ],
  );
  const desktopContextKey = JSON.stringify(desktopContext);

  useEffect(() => {
    if (lastPublishedKeyRef.current === desktopContextKey) {
      return;
    }

    const api = readNativeApi();
    if (!api) {
      return;
    }

    lastPublishedKeyRef.current = desktopContextKey;
    void api.server
      .setDesktopContext({
        ...desktopContext,
        updatedAt: new Date().toISOString(),
      })
      .catch((error) => {
        if (lastPublishedKeyRef.current === desktopContextKey) {
          lastPublishedKeyRef.current = null;
        }
        console.warn("Failed to publish desktop context", error);
      });
  }, [desktopContext, desktopContextKey]);
}
