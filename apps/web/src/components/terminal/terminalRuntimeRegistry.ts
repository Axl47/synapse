// FILE: terminalRuntimeRegistry.ts
// Purpose: Keep a stable runtime map and delegate terminal lifecycle work to terminalRuntime.ts.
// Layer: Terminal runtime infrastructure
// Depends on: terminalRuntime.ts for lifecycle, terminalRuntimeTypes.ts for stable ids and contracts.

import { SearchAddon } from "@xterm/addon-search";
import { Terminal } from "@xterm/xterm";

import {
  attachRuntimeToContainer,
  createRuntimeEntry,
  detachRuntimeFromContainer,
  disposeRuntimeEntry,
  syncRuntimeConfig,
  updateRuntimeViewState,
} from "./terminalRuntime";
import type {
  TerminalRuntimeConfig,
  TerminalRuntimeEntry,
  TerminalRuntimeStatus,
  TerminalRuntimeViewState,
} from "./terminalRuntimeTypes";
import { buildTerminalRuntimeKey } from "./terminalRuntimeTypes";

export { buildTerminalRuntimeKey, type TerminalRuntimeCallbacks } from "./terminalRuntimeTypes";

// --- Registry orchestration -------------------------------------------------

class TerminalRuntimeRegistry {
  private entries = new Map<string, TerminalRuntimeEntry>();
  private scheduledDisposals = new Map<
    string,
    { frameId: number | null; timeoutId: number | null }
  >();

  private cancelScheduledDispose(runtimeKey: string): void {
    const scheduled = this.scheduledDisposals.get(runtimeKey);
    if (!scheduled) return;
    if (scheduled.frameId !== null) {
      window.cancelAnimationFrame(scheduled.frameId);
    }
    if (scheduled.timeoutId !== null) {
      window.clearTimeout(scheduled.timeoutId);
    }
    this.scheduledDisposals.delete(runtimeKey);
  }

  attach(
    config: TerminalRuntimeConfig,
    viewState: TerminalRuntimeViewState,
    container: HTMLDivElement,
  ): { terminal: Terminal; searchAddon: SearchAddon; runtimeStatus: TerminalRuntimeStatus } {
    this.cancelScheduledDispose(config.runtimeKey);
    let entry = this.entries.get(config.runtimeKey);
    if (!entry) {
      entry = createRuntimeEntry(config);
      this.entries.set(config.runtimeKey, entry);
    } else {
      syncRuntimeConfig(entry, config);
    }

    attachRuntimeToContainer(entry, viewState, container);
    return {
      terminal: entry.terminal,
      searchAddon: entry.searchAddon,
      runtimeStatus: entry.runtimeStatus,
    };
  }

  syncConfig(runtimeKey: string, config: TerminalRuntimeConfig): void {
    const entry = this.entries.get(runtimeKey);
    if (!entry) return;
    syncRuntimeConfig(entry, config);
  }

  setViewState(runtimeKey: string, viewState: TerminalRuntimeViewState): void {
    const entry = this.entries.get(runtimeKey);
    if (!entry) return;
    updateRuntimeViewState(entry, viewState);
  }

  detach(runtimeKey: string): void {
    if (this.scheduledDisposals.has(runtimeKey)) return;
    const entry = this.entries.get(runtimeKey);
    if (!entry) return;
    detachRuntimeFromContainer(entry);
  }

  dispose(runtimeKey: string): void {
    this.cancelScheduledDispose(runtimeKey);
    const entry = this.entries.get(runtimeKey);
    if (!entry) return;
    disposeRuntimeEntry(entry);
    this.entries.delete(runtimeKey);
  }

  scheduleDispose(runtimeKey: string): void {
    if (this.scheduledDisposals.has(runtimeKey)) return;
    const scheduled = { frameId: null as number | null, timeoutId: null as number | null };
    scheduled.frameId = window.requestAnimationFrame(() => {
      scheduled.frameId = null;
      scheduled.timeoutId = window.setTimeout(() => {
        scheduled.timeoutId = null;
        this.scheduledDisposals.delete(runtimeKey);
        this.dispose(runtimeKey);
      }, 0);
    });
    this.scheduledDisposals.set(runtimeKey, scheduled);
  }

  disposeTerminal(threadId: string, terminalId: string): void {
    this.dispose(buildTerminalRuntimeKey(threadId, terminalId));
  }

  scheduleDisposeTerminal(threadId: string, terminalId: string): void {
    this.scheduleDispose(buildTerminalRuntimeKey(threadId, terminalId));
  }

  disposeThread(threadId: string): void {
    for (const runtimeKey of [...this.entries.keys()]) {
      if (runtimeKey.startsWith(`${threadId}::`)) {
        this.dispose(runtimeKey);
      }
    }
  }

  focus(runtimeKey: string): void {
    this.entries.get(runtimeKey)?.terminal.focus();
  }
}

export const terminalRuntimeRegistry = new TerminalRuntimeRegistry();
