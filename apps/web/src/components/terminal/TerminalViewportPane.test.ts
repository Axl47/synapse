import { describe, expect, it } from "vitest";

import { shouldRenderTerminalPaneChrome } from "./TerminalViewportPane";

describe("shouldRenderTerminalPaneChrome", () => {
  it("hides the duplicated root pane chrome for a single terminal", () => {
    expect(
      shouldRenderTerminalPaneChrome({
        isRootPane: true,
        terminalCount: 1,
        suppressSingleTerminalPaneChrome: true,
      }),
    ).toBe(false);
  });

  it("keeps pane chrome for terminal tabs and split panes", () => {
    expect(
      shouldRenderTerminalPaneChrome({
        isRootPane: true,
        terminalCount: 2,
        suppressSingleTerminalPaneChrome: true,
      }),
    ).toBe(true);

    expect(
      shouldRenderTerminalPaneChrome({
        isRootPane: false,
        terminalCount: 1,
        suppressSingleTerminalPaneChrome: true,
      }),
    ).toBe(true);
  });

  it("keeps the existing pane chrome when suppression is not requested", () => {
    expect(
      shouldRenderTerminalPaneChrome({
        isRootPane: true,
        terminalCount: 1,
        suppressSingleTerminalPaneChrome: false,
      }),
    ).toBe(true);
  });
});
