// FILE: desktopInstanceDescriptor.ts
// Purpose: Writes a discoverable local desktop backend descriptor for external apps.
// Layer: Desktop runtime integration

import * as FS from "node:fs";
import * as Path from "node:path";

export const DESKTOP_INSTANCE_DESCRIPTOR_TTL_MS = 15_000;
export const DESKTOP_INSTANCE_DESCRIPTOR_REFRESH_MS = 5_000;

export interface DesktopInstanceDescriptor {
  readonly version: 1;
  readonly instanceId: string;
  readonly pid: number;
  readonly mode: "desktop";
  readonly cwd: string;
  readonly stateDir: string;
  readonly host: string;
  readonly port: number;
  readonly wsUrl: string;
  readonly startedAt: string;
  readonly updatedAt: string;
  readonly expiresAt: string;
}

export interface CreateDesktopInstanceDescriptorInput {
  readonly instanceId: string;
  readonly pid: number;
  readonly cwd: string;
  readonly stateDir: string;
  readonly port: number;
  readonly wsUrl: string;
  readonly startedAt: string;
  readonly now?: Date;
  readonly ttlMs?: number;
}

export function desktopInstancesDirectory(baseDir: string): string {
  return Path.join(baseDir, "instances");
}

export function desktopInstanceDescriptorPath(baseDir: string, instanceId: string): string {
  return Path.join(desktopInstancesDirectory(baseDir), `${instanceId}.json`);
}

export function createDesktopInstanceDescriptor(
  input: CreateDesktopInstanceDescriptorInput,
): DesktopInstanceDescriptor {
  const now = input.now ?? new Date();
  const ttlMs = input.ttlMs ?? DESKTOP_INSTANCE_DESCRIPTOR_TTL_MS;
  return {
    version: 1,
    instanceId: input.instanceId,
    pid: input.pid,
    mode: "desktop",
    cwd: input.cwd,
    stateDir: input.stateDir,
    host: "127.0.0.1",
    port: input.port,
    wsUrl: input.wsUrl,
    startedAt: input.startedAt,
    updatedAt: now.toISOString(),
    expiresAt: new Date(now.getTime() + ttlMs).toISOString(),
  };
}

export function writeDesktopInstanceDescriptor(
  baseDir: string,
  descriptor: DesktopInstanceDescriptor,
): void {
  const directory = desktopInstancesDirectory(baseDir);
  FS.mkdirSync(directory, { recursive: true });
  const destination = desktopInstanceDescriptorPath(baseDir, descriptor.instanceId);
  const temporary = `${destination}.${process.pid}.${Date.now()}.tmp`;
  FS.writeFileSync(temporary, `${JSON.stringify(descriptor, null, 2)}\n`, "utf8");
  FS.renameSync(temporary, destination);
}

export function removeDesktopInstanceDescriptor(baseDir: string, instanceId: string): void {
  try {
    FS.rmSync(desktopInstanceDescriptorPath(baseDir, instanceId), { force: true });
  } catch {
    // Descriptor removal should never block app shutdown.
  }
}
