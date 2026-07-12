import {
  DEFAULT_SERVER_SETTINGS,
  ThreadId,
  type ProviderInstanceId,
} from "@synara/contracts";
import { Effect, Option } from "effect";
import { describe, expect, it, vi } from "vitest";

import { makeCodexAdoptedThreadHistorySync } from "./codexAdoptedThreadHistorySync";

const threadId = ThreadId.makeUnsafe("local-1");

function binding(runtimePayload: unknown) {
  return {
    threadId,
    provider: "codex" as const,
    providerInstanceId: "codex" as ProviderInstanceId,
    resumeCursor: { threadId: "external-1" },
    runtimePayload,
  };
}

describe("makeCodexAdoptedThreadHistorySync", () => {
  it("ignores native Codex bindings", async () => {
    const readExternalThread = vi.fn();
    const dispatch = vi.fn();
    const sync = makeCodexAdoptedThreadHistorySync({
      orchestrationEngine: { dispatch } as never,
      providerAdapterRegistry: {
        getByInstance: () => Effect.succeed({ readExternalThread } as never),
      } as never,
      providerSessionDirectory: {
        getBinding: () => Effect.succeed(Option.some(binding({}))),
      } as never,
      serverSettings: { getSettings: Effect.succeed(DEFAULT_SERVER_SETTINGS) } as never,
    });

    await expect(Effect.runPromise(sync(threadId))).resolves.toBe(false);
    expect(readExternalThread).not.toHaveBeenCalled();
    expect(dispatch).not.toHaveBeenCalled();
  });

  it("replays stable projections for adopted Codex history", async () => {
    const dispatch = vi.fn(() => Effect.succeed({ sequence: 1 }));
    const readExternalThread = vi.fn(() =>
      Effect.succeed({
        threadId: ThreadId.makeUnsafe("external-1"),
        turns: [
          {
            id: "turn-1",
            items: [{ id: "item-1", type: "agentMessage", text: "Persisted reply" }],
          },
        ],
      }),
    );
    const sync = makeCodexAdoptedThreadHistorySync({
      orchestrationEngine: { dispatch } as never,
      providerAdapterRegistry: {
        getByInstance: () => Effect.succeed({ readExternalThread } as never),
      } as never,
      providerSessionDirectory: {
        getBinding: () =>
          Effect.succeed(Option.some(binding({ adoptedExternalThread: true }))),
      } as never,
      serverSettings: { getSettings: Effect.succeed(DEFAULT_SERVER_SETTINGS) } as never,
    });

    await expect(Effect.runPromise(sync(threadId))).resolves.toBe(true);
    await expect(Effect.runPromise(sync(threadId))).resolves.toBe(true);
    expect(dispatch).toHaveBeenCalledTimes(2);
    const firstMessages = dispatch.mock.calls[0]?.[0].messages;
    const secondMessages = dispatch.mock.calls[1]?.[0].messages;
    expect(firstMessages?.map((message: { messageId: string }) => message.messageId)).toEqual(
      secondMessages?.map((message: { messageId: string }) => message.messageId),
    );
    expect(firstMessages).toMatchObject([
      { messageId: "import:local-1:codex:turn-1:item-1", text: "Persisted reply" },
    ]);
  });
});
