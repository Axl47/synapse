import { ProjectId, ThreadId } from "@t3tools/contracts";
import { describe, expect, it } from "vitest";

import { resolveDesktopContextInput } from "./desktopContext";
import type { FocusedChatContext } from "./focusedChatContext";
import type { Project, Thread } from "./types";

const PROJECT_ID = ProjectId.makeUnsafe("project-1");
const THREAD_ID = ThreadId.makeUnsafe("thread-1");
const DRAFT_THREAD_ID = ThreadId.makeUnsafe("thread-draft");

const project = {
  id: PROJECT_ID,
  name: "Pragma",
  remoteName: null,
  cwd: "/repo/pragma",
} as Project;

const thread = {
  id: THREAD_ID,
  projectId: PROJECT_ID,
  title: "Implementation",
} as Thread;

function makeContext(overrides: Partial<FocusedChatContext> = {}): FocusedChatContext {
  return {
    routeThreadId: THREAD_ID,
    splitView: null,
    focusedThreadId: THREAD_ID,
    activeThread: thread,
    activeDraftThread: null,
    activeProject: project,
    activeProjectId: PROJECT_ID,
    ...overrides,
  };
}

describe("resolveDesktopContextInput", () => {
  it("publishes the focused project and thread", () => {
    expect(resolveDesktopContextInput(makeContext())).toEqual({
      projectId: PROJECT_ID,
      projectTitle: "Pragma",
      workspaceRoot: "/repo/pragma",
      threadId: THREAD_ID,
      threadTitle: "Implementation",
    });
  });

  it("uses the focused draft thread id without inventing a title", () => {
    expect(
      resolveDesktopContextInput(
        makeContext({
          focusedThreadId: DRAFT_THREAD_ID,
          activeThread: null,
          activeDraftThread: { projectId: PROJECT_ID } as FocusedChatContext["activeDraftThread"],
        }),
      ),
    ).toEqual({
      projectId: PROJECT_ID,
      projectTitle: "Pragma",
      workspaceRoot: "/repo/pragma",
      threadId: DRAFT_THREAD_ID,
      threadTitle: null,
    });
  });

  it("clears all fields when no project is focused", () => {
    expect(
      resolveDesktopContextInput(
        makeContext({
          focusedThreadId: null,
          activeThread: null,
          activeDraftThread: null,
          activeProject: null,
          activeProjectId: null,
        }),
      ),
    ).toEqual({
      projectId: null,
      projectTitle: null,
      workspaceRoot: null,
      threadId: null,
      threadTitle: null,
    });
  });
});
