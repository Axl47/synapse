// FILE: composerDraftImportActivation.ts
// Purpose: Validates and queues opaque composer draft import activation URLs.
// Layer: Desktop runtime integration

export const COMPOSER_DRAFT_IMPORT_CAPABILITY = "composer-draft-import-v1";
export const COMPOSER_DRAFT_IMPORT_ACTIVATION_HOST = "composer-draft";
export const MAX_PENDING_COMPOSER_DRAFT_IMPORT_INTENTS = 16;

const COMPOSER_DRAFT_IMPORT_ID_PATTERN = /^cdi_[a-z0-9]+$/;

export function composerDraftImportActivationBaseUrl(scheme: string): string {
  return `${scheme}://${COMPOSER_DRAFT_IMPORT_ACTIVATION_HOST}/`;
}

export function parseComposerDraftImportActivationUrl(
  rawUrl: string,
  expectedScheme: string,
): string | null {
  const prefix = composerDraftImportActivationBaseUrl(expectedScheme);
  if (!rawUrl.startsWith(prefix)) {
    return null;
  }

  const importId = rawUrl.slice(prefix.length);
  if (importId.length === 0 || importId.length > 128) {
    return null;
  }
  return COMPOSER_DRAFT_IMPORT_ID_PATTERN.test(importId) ? importId : null;
}

export function shouldRegisterComposerDraftImportProtocol(isPackaged: boolean): boolean {
  return isPackaged;
}

export type ComposerDraftImportEnqueueResult = "queued" | "duplicate" | "full";

export class ComposerDraftImportIntentQueue {
  readonly #maxPending: number;
  readonly #pending: string[] = [];
  readonly #pendingSet = new Set<string>();

  constructor(maxPending = MAX_PENDING_COMPOSER_DRAFT_IMPORT_INTENTS) {
    this.#maxPending = Math.max(1, maxPending);
  }

  get size(): number {
    return this.#pending.length;
  }

  enqueue(importId: string): ComposerDraftImportEnqueueResult {
    if (this.#pendingSet.has(importId)) {
      return "duplicate";
    }
    if (this.#pending.length >= this.#maxPending) {
      return "full";
    }
    this.#pending.push(importId);
    this.#pendingSet.add(importId);
    return "queued";
  }

  drain(deliver: (importId: string) => boolean): number {
    let delivered = 0;
    while (this.#pending.length > 0) {
      const importId = this.#pending[0];
      if (!importId || !deliver(importId)) {
        break;
      }
      this.#pending.shift();
      this.#pendingSet.delete(importId);
      delivered += 1;
    }
    return delivered;
  }
}
