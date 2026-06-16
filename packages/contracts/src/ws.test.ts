import { assert, it } from "@effect/vitest";
import { Effect, Schema } from "effect";

import { ORCHESTRATION_WS_CHANNELS, ORCHESTRATION_WS_METHODS } from "./orchestration";
import { WebSocketRequest, WsResponse, WS_CHANNELS, WS_METHODS } from "./ws";

const decode = <S extends Schema.Top>(
  schema: S,
  input: unknown,
): Effect.Effect<Schema.Schema.Type<S>, Schema.SchemaError, never> =>
  Schema.decodeUnknownEffect(schema as never)(input) as Effect.Effect<
    Schema.Schema.Type<S>,
    Schema.SchemaError,
    never
  >;

it.effect("accepts getTurnDiff requests when fromTurnCount <= toTurnCount", () =>
  Effect.gen(function* () {
    const parsed = yield* decode(WebSocketRequest, {
      id: "req-1",
      body: {
        _tag: ORCHESTRATION_WS_METHODS.getTurnDiff,
        threadId: "thread-1",
        fromTurnCount: 1,
        toTurnCount: 2,
      },
    });
    assert.strictEqual(parsed.body._tag, ORCHESTRATION_WS_METHODS.getTurnDiff);
  }),
);

it.effect("rejects getTurnDiff requests when fromTurnCount > toTurnCount", () =>
  Effect.gen(function* () {
    const result = yield* Effect.exit(
      decode(WebSocketRequest, {
        id: "req-1",
        body: {
          _tag: ORCHESTRATION_WS_METHODS.getTurnDiff,
          threadId: "thread-1",
          fromTurnCount: 3,
          toTurnCount: 2,
        },
      }),
    );
    assert.strictEqual(result._tag, "Failure");
  }),
);

it.effect("trims websocket request id and nested orchestration ids", () =>
  Effect.gen(function* () {
    const parsed = yield* decode(WebSocketRequest, {
      id: " req-1 ",
      body: {
        _tag: ORCHESTRATION_WS_METHODS.getTurnDiff,
        threadId: " thread-1 ",
        fromTurnCount: 0,
        toTurnCount: 0,
      },
    });
    assert.strictEqual(parsed.id, "req-1");
    assert.strictEqual(parsed.body._tag, ORCHESTRATION_WS_METHODS.getTurnDiff);
    if (parsed.body._tag === ORCHESTRATION_WS_METHODS.getTurnDiff) {
      assert.strictEqual(parsed.body.threadId, "thread-1");
    }
  }),
);

it.effect("accepts git.preparePullRequestThread requests", () =>
  Effect.gen(function* () {
    const parsed = yield* decode(WebSocketRequest, {
      id: "req-pr-1",
      body: {
        _tag: WS_METHODS.gitPreparePullRequestThread,
        cwd: "/repo",
        reference: "#42",
        mode: "worktree",
      },
    });
    assert.strictEqual(parsed.body._tag, WS_METHODS.gitPreparePullRequestThread);
  }),
);

it.effect("accepts project script discovery requests", () =>
  Effect.gen(function* () {
    const parsed = yield* decode(WebSocketRequest, {
      id: "req-project-scripts-1",
      body: {
        _tag: WS_METHODS.projectsDiscoverScripts,
        cwd: "/repo",
        depth: 1,
      },
    });
    assert.strictEqual(parsed.body._tag, WS_METHODS.projectsDiscoverScripts);
  }),
);

it.effect("accepts desktop context server requests", () =>
  Effect.gen(function* () {
    const setContext = yield* decode(WebSocketRequest, {
      id: "req-desktop-context-set",
      body: {
        _tag: WS_METHODS.serverSetDesktopContext,
        projectId: " project-1 ",
        projectTitle: " Pragma ",
        workspaceRoot: " /repo/pragma ",
        threadId: " thread-1 ",
        threadTitle: " Implementation ",
        updatedAt: "2026-06-16T04:00:00.000Z",
      },
    });
    assert.strictEqual(setContext.body._tag, WS_METHODS.serverSetDesktopContext);
    if (setContext.body._tag === WS_METHODS.serverSetDesktopContext) {
      assert.strictEqual(setContext.body.projectId, "project-1");
      assert.strictEqual(setContext.body.projectTitle, "Pragma");
      assert.strictEqual(setContext.body.threadId, "thread-1");
    }

    const getContext = yield* decode(WebSocketRequest, {
      id: "req-desktop-context-get",
      body: { _tag: WS_METHODS.serverGetDesktopContext },
    });
    assert.strictEqual(getContext.body._tag, WS_METHODS.serverGetDesktopContext);

    const subscribeContext = yield* decode(WebSocketRequest, {
      id: "req-desktop-context-subscribe",
      body: { _tag: WS_METHODS.subscribeServerDesktopContext },
    });
    assert.strictEqual(subscribeContext.body._tag, WS_METHODS.subscribeServerDesktopContext);
  }),
);

it.effect("accepts typed websocket push envelopes with sequence", () =>
  Effect.gen(function* () {
    const parsed = yield* decode(WsResponse, {
      type: "push",
      sequence: 1,
      channel: WS_CHANNELS.serverWelcome,
      data: {
        cwd: "/tmp/workspace",
        projectName: "workspace",
      },
    });

    if (!("type" in parsed) || parsed.type !== "push") {
      assert.fail("expected websocket response to decode as a push envelope");
    }

    assert.strictEqual(parsed.type, "push");
    assert.strictEqual(parsed.sequence, 1);
    assert.strictEqual(parsed.channel, WS_CHANNELS.serverWelcome);
  }),
);

it.effect("accepts git.actionProgress push envelopes", () =>
  Effect.gen(function* () {
    const parsed = yield* decode(WsResponse, {
      type: "push",
      sequence: 3,
      channel: WS_CHANNELS.gitActionProgress,
      data: {
        actionId: "action-1",
        cwd: "/repo",
        action: "commit",
        kind: "phase_started",
        phase: "commit",
        label: "Committing...",
      },
    });

    if (!("type" in parsed) || parsed.type !== "push") {
      assert.fail("expected websocket response to decode as a push envelope");
    }

    assert.strictEqual(parsed.channel, WS_CHANNELS.gitActionProgress);
  }),
);

it.effect("rejects push envelopes when channel payload does not match the channel schema", () =>
  Effect.gen(function* () {
    const result = yield* Effect.exit(
      decode(WsResponse, {
        type: "push",
        sequence: 2,
        channel: ORCHESTRATION_WS_CHANNELS.domainEvent,
        data: {
          cwd: "/tmp/workspace",
          projectName: "workspace",
        },
      }),
    );

    assert.strictEqual(result._tag, "Failure");
  }),
);
