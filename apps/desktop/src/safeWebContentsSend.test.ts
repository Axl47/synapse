// FILE: safeWebContentsSend.test.ts
// Purpose: Verifies stale renderer IPC sends are ignored while real send failures still surface.

import type { WebContents } from "electron";
import { describe, expect, it, vi } from "vitest";

import { safeSendToWebContents } from "./safeWebContentsSend";

function createWebContentsStub(input: {
  isDestroyed?: boolean;
  send?: (...args: unknown[]) => void;
}): WebContents {
  return {
    isDestroyed: () => input.isDestroyed === true,
    send: input.send ?? vi.fn(),
  } as unknown as WebContents;
}

describe("safeSendToWebContents", () => {
  it("sends to live webContents", () => {
    const send = vi.fn();
    const webContents = createWebContentsStub({ send });

    expect(safeSendToWebContents(webContents, "desktop:test", { value: 1 })).toBe(true);
    expect(send).toHaveBeenCalledWith("desktop:test", { value: 1 });
  });

  it("skips missing or destroyed webContents", () => {
    const send = vi.fn();
    const webContents = createWebContentsStub({ isDestroyed: true, send });

    expect(safeSendToWebContents(null, "desktop:test")).toBe(false);
    expect(safeSendToWebContents(webContents, "desktop:test")).toBe(false);
    expect(send).not.toHaveBeenCalled();
  });

  it("ignores stale renderer frame errors", () => {
    const webContents = createWebContentsStub({
      send: () => {
        throw new Error("Render frame was disposed before WebFrameMain could be accessed");
      },
    });

    expect(safeSendToWebContents(webContents, "desktop:test")).toBe(false);
  });

  it("rethrows unexpected send errors", () => {
    const webContents = createWebContentsStub({
      send: () => {
        throw new Error("permission denied");
      },
    });

    expect(() => safeSendToWebContents(webContents, "desktop:test")).toThrow("permission denied");
  });
});
