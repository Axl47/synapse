import { describe, expect, it } from "vitest";
import { isUltraEffort, ULTRA_EFFORT_TEXT_CLASS_NAME } from "./effortPresentation";

describe("Ultra effort presentation", () => {
  it("accents only the Ultra Codex effort", () => {
    expect(isUltraEffort("ultra")).toBe(true);
    expect(isUltraEffort("Ultra")).toBe(true);
    expect(isUltraEffort("max")).toBe(false);
    expect(ULTRA_EFFORT_TEXT_CLASS_NAME).toContain("violet");
  });
});
