import { type ModelSelection } from "@synara/contracts";
import { describe, expect, it } from "vitest";
import {
  resolveAvailableHandoffTargetProviders,
  resolveThreadHandoffTitle,
  resolveThreadHandoffModelSelection,
} from "./threadHandoff";

describe("threadHandoff", () => {
  it("lists all supported handoff targets except the active provider", () => {
    const providers = [
      "codex",
      "claudeAgent",
      "cursor",
      "antigravity",
      "grok",
      "droid",
      "kilo",
      "opencode",
      "pi",
    ] as const;

    for (const source of providers) {
      expect(resolveAvailableHandoffTargetProviders(source)).toEqual(
        providers.filter((provider) => provider !== source),
      );
    }
  });

  it("preserves the source thread title for the created handoff thread", () => {
    expect(resolveThreadHandoffTitle({ title: "General Greeting" })).toBe("General Greeting");
    expect(resolveThreadHandoffTitle({ title: "  Debug   Grok handoff  " })).toBe(
      "Debug Grok handoff",
    );
  });

  it("prefers sticky model selection for the chosen handoff target", () => {
    const stickySelection = {
      instanceId: "antigravity_work",
      model: "gemini-3.1-pro-preview",
    } satisfies ModelSelection;

    expect(
      resolveThreadHandoffModelSelection({
        sourceThread: {
          modelSelection: {
            instanceId: "claudeAgent",
            model: "claude-sonnet-4-6",
          },
        },
        targetProvider: "antigravity",
        targetProviderInstanceId: "antigravity_work",
        projectDefaultModelSelection: {
          instanceId: "antigravity",
          model: "gemini-3.1-pro-preview",
        },
        stickyModelSelectionByProvider: {
          antigravity_work: stickySelection,
        },
      }),
    ).toEqual({ ...stickySelection, instanceId: "antigravity_work" });
  });

  it("does not borrow provider-only sticky selections for a custom target instance", () => {
    expect(
      resolveThreadHandoffModelSelection({
        sourceThread: {
          modelSelection: {
            instanceId: "claudeAgent",
            model: "claude-sonnet-4-6",
          },
        },
        targetProvider: "antigravity",
        targetProviderInstanceId: "antigravity_work",
        projectDefaultModelSelection: null,
        stickyModelSelectionByProvider: {
          antigravity: {
            instanceId: "antigravity",
            model: "gemini-3.1-pro-preview",
          },
        },
      }),
    ).toEqual({
      instanceId: "antigravity_work",
      model: "Gemini 3.5 Flash",
    });
  });

  it("adds the chosen target instance id to project-default handoff selections", () => {
    expect(
      resolveThreadHandoffModelSelection({
        sourceThread: {
          modelSelection: {
            instanceId: "codex",
            model: "gpt-5.4",
          },
        },
        targetProvider: "claudeAgent",
        targetProviderInstanceId: "claude_work",
        projectDefaultModelSelection: {
          instanceId: "claudeAgent",
          model: "claude-sonnet-4-6",
        },
        stickyModelSelectionByProvider: {},
      }),
    ).toEqual({
      instanceId: "claude_work",
      model: "claude-sonnet-5",
    });
  });

  it("falls back to the resolved provider default model when no sticky or project default exists", () => {
    expect(
      resolveThreadHandoffModelSelection({
        sourceThread: {
          modelSelection: {
            instanceId: "antigravity",
            model: "gemini-3.1-pro-preview",
          },
        },
        targetProvider: "codex",
        targetProviderInstanceId: "codex_personal",
        projectDefaultModelSelection: null,
        stickyModelSelectionByProvider: {},
      }),
    ).toEqual({
      instanceId: "codex_personal",
      model: "gpt-5.6-sol",
    });
  });
});
