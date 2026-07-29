import { describe, expect, it, vi } from "vitest";

import {
  ComposerDraftImportIntentQueue,
  composerDraftImportActivationBaseUrl,
  parseComposerDraftImportActivationUrl,
  shouldRegisterComposerDraftImportProtocol,
} from "./composerDraftImportActivation";

describe("composer draft import activation", () => {
  it("accepts only the flavor-specific opaque import URL", () => {
    expect(
      parseComposerDraftImportActivationUrl("synara://composer-draft/cdi_abc123", "synara"),
    ).toBe("cdi_abc123");
    expect(
      parseComposerDraftImportActivationUrl(
        "synara-canary://composer-draft/cdi_abc123",
        "synara-canary",
      ),
    ).toBe("cdi_abc123");
    expect(composerDraftImportActivationBaseUrl("synara-canary")).toBe(
      "synara-canary://composer-draft/",
    );
  });

  it.each([
    "https://composer-draft/cdi_abc123",
    "synara://app/cdi_abc123",
    "synara://composer-draft/",
    "synara://composer-draft/not-an-import",
    "synara://composer-draft/cdi_UPPER",
    "synara://composer-draft/cdi_abc123/asset.png",
    "synara://composer-draft/cdi_abc123?token=secret",
    "synara://composer-draft/cdi_abc123#fragment",
    "synara://token@composer-draft/cdi_abc123",
    "synara://composer-draft/%2Ftmp%2Fsecret",
  ])("rejects malformed or data-bearing activation URL %s", (url) => {
    expect(parseComposerDraftImportActivationUrl(url, "synara")).toBeNull();
  });

  it("registers the operating-system protocol handler only for packaged builds", () => {
    expect(shouldRegisterComposerDraftImportProtocol(true)).toBe(true);
    expect(shouldRegisterComposerDraftImportProtocol(false)).toBe(false);
  });

  it("retains cold-start intents until delivery and drains them in FIFO order", () => {
    const queue = new ComposerDraftImportIntentQueue();
    queue.enqueue("cdi_first");
    queue.enqueue("cdi_second");

    expect(queue.drain(() => false)).toBe(0);
    expect(queue.size).toBe(2);

    const delivered: string[] = [];
    expect(
      queue.drain((importId) => {
        delivered.push(importId);
        return true;
      }),
    ).toBe(2);
    expect(delivered).toEqual(["cdi_first", "cdi_second"]);
  });

  it("supports warm delivery, suppresses pending duplicates, and focuses per activation", () => {
    const queue = new ComposerDraftImportIntentQueue();
    const deliver = vi.fn(() => true);
    const focus = vi.fn();
    const activate = (url: string) => {
      const importId = parseComposerDraftImportActivationUrl(url, "synara");
      if (!importId) return false;
      queue.enqueue(importId);
      queue.drain(deliver);
      focus();
      return true;
    };

    expect(activate("synara://composer-draft/cdi_warm")).toBe(true);
    expect(deliver).toHaveBeenCalledWith("cdi_warm");
    expect(focus).toHaveBeenCalledOnce();

    const blockedQueue = new ComposerDraftImportIntentQueue();
    expect(blockedQueue.enqueue("cdi_duplicate")).toBe("queued");
    expect(blockedQueue.enqueue("cdi_duplicate")).toBe("duplicate");
    expect(blockedQueue.size).toBe(1);
  });

  it("reports a bounded-queue conflict without overwriting pending intents", () => {
    const queue = new ComposerDraftImportIntentQueue(1);
    expect(queue.enqueue("cdi_first")).toBe("queued");
    expect(queue.enqueue("cdi_second")).toBe("full");

    const delivered: string[] = [];
    queue.drain((importId) => {
      delivered.push(importId);
      return true;
    });
    expect(delivered).toEqual(["cdi_first"]);
  });
});
