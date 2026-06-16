import { Effect } from "effect";
import { describe, expect, it } from "vitest";

import { DesktopContext, DesktopContextLive, emptyDesktopContext } from "./desktopContext";

const runWithDesktopContext = <A, E>(effect: Effect.Effect<A, E, DesktopContext>) =>
  Effect.runPromise(effect.pipe(Effect.provide(DesktopContextLive)));

describe("DesktopContext", () => {
  it("starts empty and stores the latest focused desktop context", async () => {
    const result = await runWithDesktopContext(
      Effect.gen(function* () {
        const desktopContext = yield* DesktopContext;
        const initial = yield* desktopContext.get;
        const updated = yield* desktopContext.set({
          projectId: "project-1" as never,
          projectTitle: " Pragma ",
          workspaceRoot: " /repo/pragma ",
          threadId: "thread-1" as never,
          threadTitle: " Follow Synapse ",
          updatedAt: "2026-06-16T04:00:00.000Z",
        });
        const stored = yield* desktopContext.get;
        return { initial, updated, stored };
      }),
    );

    expect(result.initial).toEqual(emptyDesktopContext);
    expect(result.updated).toEqual({
      projectId: "project-1",
      projectTitle: "Pragma",
      workspaceRoot: "/repo/pragma",
      threadId: "thread-1",
      threadTitle: "Follow Synapse",
      updatedAt: "2026-06-16T04:00:00.000Z",
    });
    expect(result.stored).toEqual(result.updated);
  });
});
