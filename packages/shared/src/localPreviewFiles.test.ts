import { describe, expect, it } from "vitest";

import {
  isSupportedLocalHtmlPath,
  isSupportedLocalPreviewFilePath,
} from "./localPreviewFiles";

describe("local preview file allowlist", () => {
  it("allows html files for explicit browser previews", () => {
    expect(isSupportedLocalHtmlPath("docs/plan.html")).toBe(true);
    expect(isSupportedLocalHtmlPath("docs/plan.HTML")).toBe(true);
    expect(isSupportedLocalPreviewFilePath("docs/plan.html")).toBe(true);
  });
});
