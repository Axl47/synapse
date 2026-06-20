// FILE: safeWebContentsSend.ts
// Purpose: Sends renderer IPC without surfacing expected stale-frame races.
// Layer: Desktop main-process utility
// Depends on: Electron WebContents

import type { WebContents } from "electron";

const STALE_WEB_CONTENTS_SEND_ERROR_PATTERNS = [
  "Render frame was disposed",
  "WebFrameMain could be accessed",
  "WebContents was destroyed",
  "Object has been destroyed",
] as const;

export function safeSendToWebContents(
  webContents: WebContents | null | undefined,
  channel: string,
  ...args: unknown[]
): boolean {
  if (!webContents || webContents.isDestroyed()) {
    return false;
  }

  try {
    webContents.send(channel, ...args);
    return true;
  } catch (error) {
    if (isStaleWebContentsSendError(error)) {
      return false;
    }
    throw error;
  }
}

function isStaleWebContentsSendError(error: unknown): boolean {
  const message = error instanceof Error ? error.message : String(error);
  return STALE_WEB_CONTENTS_SEND_ERROR_PATTERNS.some((pattern) => message.includes(pattern));
}
