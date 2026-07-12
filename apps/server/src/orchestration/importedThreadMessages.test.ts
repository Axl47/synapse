import { ThreadId } from "@synara/contracts";
import { describe, expect, it } from "vitest";

import { mapCodexSnapshotMessages } from "./importedThreadMessages";

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
