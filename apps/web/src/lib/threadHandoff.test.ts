import {
  EventId,
  MessageId,
  type ModelSelection,
  type OrchestrationThreadActivity,
} from "@synara/contracts";
import { describe, expect, it } from "vitest";
import {
  buildThreadHandoffImportedActivities,
  buildThreadHandoffImportedMessages,
  resolveAvailableHandoffTargetProviders,
  resolveAvailableHandoffTargets,
  resolveThreadHandoffTitle,
  resolveThreadHandoffModelSelection,
} from "./threadHandoff";
import { appendAssistantSelectionsToPrompt } from "./assistantSelections";
import {
  appendBrowserAnnotationsToPrompt,
  extractTrailingBrowserAnnotations,
  type BrowserAnnotationDraft,
} from "./browserAnnotations";

describe("threadHandoff", () => {
  it("strips source-thread browser annotations and selections from imported messages", () => {
    const sourceMessageId = MessageId.makeUnsafe("source-user-message");
    const annotation: BrowserAnnotationDraft = {
      id: "annotation-1",
      ordinal: 1,
      tabId: "tab-1",
      source: { url: "https://example.test/docs", pageTitle: "Docs" },
      selector: "main > button",
      tagName: "button",
      role: "button",
      name: "Save",
      text: "Save",
      fingerprint: "button|save|main",
      comment: "Remove this",
      capturedAt: "2026-07-23T10:00:00.000Z",
    };
    const text = appendBrowserAnnotationsToPrompt(
      appendAssistantSelectionsToPrompt("Update the page", [
        { assistantMessageId: "assistant-1", text: "Quoted response" },
      ]),
      [annotation],
      sourceMessageId,
    );

    const [imported] = buildThreadHandoffImportedMessages({
      messages: [
        {
          id: sourceMessageId,
          role: "user",
          text,
          createdAt: "2026-07-23T10:00:00.000Z",
          streaming: false,
          source: "native",
        },
      ],
    });
    expect(imported).toBeTruthy();
    const extracted = extractTrailingBrowserAnnotations(imported!.text, imported!.messageId);
    expect(imported!.messageId).not.toBe(sourceMessageId);
    expect(extracted.promptText).toBe("Update the page");
    expect(extracted.annotations).toEqual([]);
    expect(imported!.text).not.toContain("<browser_annotations>");
    expect(imported!.text).not.toContain("annotation-1");
    expect(imported!.text).not.toContain("<assistant_selection>");
  });

  it("does not import a source provider's configured context window", () => {
    const activity = (kind: string): OrchestrationThreadActivity => ({
      id: EventId.makeUnsafe(`activity-${kind}`),
      createdAt: "2026-07-21T00:00:00.000Z",
      tone: "info",
      kind,
      summary: kind,
      payload: {},
      turnId: null,
    });

    const imported = buildThreadHandoffImportedActivities({
      activities: [
        activity("context-window.configured"),
        activity("context-window.updated"),
        activity("tool.started"),
      ],
    });

    expect(imported.map(({ kind }) => kind)).toEqual(["context-window.updated"]);
  });

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

  it("orders enabled handoff instances by the shared provider order", () => {
    expect(
      resolveAvailableHandoffTargets({
        sourceProvider: "codex",
        sourceProviderInstanceId: "codex",
        providerInstances: [
          {
            provider: "claudeAgent",
            instanceId: "claude_work",
            label: "Work",
            enabled: true,
            isDefault: false,
          },
          {
            provider: "codex",
            instanceId: "codex",
            label: "Codex",
            enabled: true,
            isDefault: true,
          },
          {
            provider: "claudeAgent",
            instanceId: "claudeAgent",
            label: "Claude",
            enabled: true,
            isDefault: true,
          },
          {
            provider: "cursor",
            instanceId: "cursor",
            label: "Cursor",
            enabled: false,
            isDefault: true,
          },
        ],
      }),
    ).toEqual([
      {
        provider: "claudeAgent",
        instanceId: "claudeAgent",
        label: "Claude",
      },
      {
        provider: "claudeAgent",
        instanceId: "claude_work",
        label: "Work",
      },
    ]);
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
