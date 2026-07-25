import { describe, expect, it } from "vitest";

import {
  geminiCapabilitiesForModel,
  geminiModelOptionsFromEffortValue,
  getGeminiThinkingConfigKind,
  getGeminiThinkingModelAlias,
  resolveGeminiApiModelId,
} from "./geminiModelCapabilities.ts";

describe("Gemini model capabilities", () => {
  it("distinguishes Gemini 3 levels from Gemini 2.5 budgets", () => {
    expect(getGeminiThinkingConfigKind("auto-gemini-3")).toBe("level");
    expect(getGeminiThinkingConfigKind("gemini-2.5-pro")).toBe("budget");
    expect(getGeminiThinkingConfigKind("gemini-2.0-flash")).toBeNull();
  });

  it("maps supported effort values to provider options", () => {
    expect(geminiModelOptionsFromEffortValue("HIGH")).toEqual({ thinkingLevel: "HIGH" });
    expect(geminiModelOptionsFromEffortValue("-1")).toEqual({ thinkingBudget: -1 });
    expect(geminiModelOptionsFromEffortValue("unsupported")).toBeUndefined();
  });

  it("builds only aliases supported by the selected model family", () => {
    expect(getGeminiThinkingModelAlias("gemini-3-pro", { thinkingLevel: "LOW" })).toBe(
      "synara-gemini-gemini-3-pro-thinking-level-low",
    );
    expect(getGeminiThinkingModelAlias("gemini-2.5-pro", { thinkingBudget: -1 })).toBe(
      "synara-gemini-gemini-2-5-pro-thinking-budget-dynamic",
    );
    expect(getGeminiThinkingModelAlias("gemini-3-pro", { thinkingBudget: 512 })).toBeNull();
  });

  it("falls back to the original API model id when no alias applies", () => {
    expect(resolveGeminiApiModelId("gemini-2.0-flash", { thinkingLevel: "HIGH" })).toBe(
      "gemini-2.0-flash",
    );
    expect(geminiCapabilitiesForModel("gemini-3-pro").reasoningEffortLevels).toHaveLength(2);
  });
});
