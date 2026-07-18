import { ThreadId } from "@synara/contracts";
import { describe, expect, it } from "vitest";

import { mapCodexSnapshotMessages, mapFactorySnapshotMessages } from "./importedThreadMessages.ts";

describe("mapCodexSnapshotMessages", () => {
  it("prefers stable provider turn and item IDs", () => {
    const messages = mapCodexSnapshotMessages({
      threadId: ThreadId.makeUnsafe("local-1"),
      importedAt: "2026-07-12T12:00:00.000Z",
      turns: [
        {
          id: "turn-1",
          items: [
            { id: "item-user", type: "userMessage", content: [{ type: "text", text: "Hi" }] },
            { id: "item-agent", type: "agentMessage", text: "Hello" },
          ],
        },
      ],
    });

    expect(messages.map((message) => message.messageId)).toEqual([
      "import:local-1:codex:turn-1:item-user",
      "import:local-1:codex:turn-1:item-agent",
    ]);
  });

  it("retains positional IDs as the compatibility fallback", () => {
    const messages = mapCodexSnapshotMessages({
      threadId: ThreadId.makeUnsafe("local-1"),
      importedAt: "2026-07-12T12:00:00.000Z",
      turns: [{ items: [{ type: "agentMessage", text: "Hello" }] }],
    });

    expect(messages[0]?.messageId).toBe("import:local-1:0:0");
  });
});

it("maps visible Factory session items and ignores unrelated rows", () => {
  const importedAt = "2026-07-08T00:00:00.000Z";
  expect(
    mapFactorySnapshotMessages({
      threadId: ThreadId.makeUnsafe("thread-1"),
      importedAt,
      turns: [
        {
          items: [
            {
              type: "factoryMessage",
              id: "user-1",
              role: "user",
              text: "Question",
              timestamp: "2026-07-07T23:59:00.000Z",
            },
            { type: "tool", text: "hidden" },
          ],
        },
        {
          items: [{ type: "factoryMessage", id: "assistant-1", role: "assistant", text: "Answer" }],
        },
      ],
    }),
  ).toEqual([
    {
      messageId: "import:thread-1:droid:0:0:user-1",
      role: "user",
      text: "Question",
      createdAt: "2026-07-07T23:59:00.000Z",
      updatedAt: "2026-07-07T23:59:00.000Z",
    },
    {
      messageId: "import:thread-1:droid:1:0:assistant-1",
      role: "assistant",
      text: "Answer",
      createdAt: "2026-07-08T00:00:00.001Z",
      updatedAt: "2026-07-08T00:00:00.001Z",
    },
  ]);
});
