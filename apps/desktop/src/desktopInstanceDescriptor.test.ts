import * as FS from "node:fs";
import * as OS from "node:os";
import * as Path from "node:path";

import { describe, expect, it } from "vitest";

import {
  createDesktopInstanceDescriptor,
  desktopInstanceDescriptorPath,
  removeDesktopInstanceDescriptor,
  writeDesktopInstanceDescriptor,
} from "./desktopInstanceDescriptor";

describe("desktopInstanceDescriptor", () => {
  it("writes and removes a discoverable desktop backend descriptor", () => {
    const root = FS.mkdtempSync(Path.join(OS.tmpdir(), "synapse-desktop-instance-"));

    try {
      const descriptor = createDesktopInstanceDescriptor({
        instanceId: "run-1",
        pid: 1234,
        cwd: "/repo/synapse",
        stateDir: "/tmp/synapse-state",
        port: 57777,
        wsUrl: "ws://127.0.0.1:57777/ws?token=secret",
        startedAt: "2026-06-16T04:00:00.000Z",
        now: new Date("2026-06-16T04:00:05.000Z"),
        ttlMs: 10_000,
      });

      writeDesktopInstanceDescriptor(root, descriptor);

      const descriptorPath = desktopInstanceDescriptorPath(root, "run-1");
      const written = JSON.parse(FS.readFileSync(descriptorPath, "utf8"));
      expect(written).toMatchObject({
        version: 1,
        instanceId: "run-1",
        mode: "desktop",
        cwd: "/repo/synapse",
        stateDir: "/tmp/synapse-state",
        host: "127.0.0.1",
        port: 57777,
        wsUrl: "ws://127.0.0.1:57777/ws?token=secret",
        startedAt: "2026-06-16T04:00:00.000Z",
        updatedAt: "2026-06-16T04:00:05.000Z",
        expiresAt: "2026-06-16T04:00:15.000Z",
      });

      removeDesktopInstanceDescriptor(root, "run-1");
      expect(FS.existsSync(descriptorPath)).toBe(false);
    } finally {
      FS.rmSync(root, { recursive: true, force: true });
    }
  });
});
