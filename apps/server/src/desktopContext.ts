import { Effect, Layer, PubSub, Ref, ServiceMap, Stream } from "effect";

import type { ServerDesktopContext, ServerSetDesktopContextInput } from "@t3tools/contracts";

export interface DesktopContextShape {
  readonly get: Effect.Effect<ServerDesktopContext>;
  readonly set: (input: ServerSetDesktopContextInput) => Effect.Effect<ServerDesktopContext>;
  readonly stream: Stream.Stream<ServerDesktopContext>;
}

export class DesktopContext extends ServiceMap.Service<DesktopContext, DesktopContextShape>()(
  "t3/desktopContext",
) {}

export const emptyDesktopContext: ServerDesktopContext = {
  projectId: null,
  projectTitle: null,
  workspaceRoot: null,
  threadId: null,
  threadTitle: null,
  updatedAt: null,
};

function normalizeOptionalText(value: string | null | undefined): string | null {
  const trimmed = value?.trim() ?? "";
  return trimmed.length > 0 ? trimmed : null;
}

function normalizeOptionalId<T extends string>(value: T | null | undefined): T | null {
  const trimmed = typeof value === "string" ? value.trim() : "";
  return trimmed.length > 0 ? (trimmed as T) : null;
}

function normalizeDesktopContext(input: ServerSetDesktopContextInput): ServerDesktopContext {
  return {
    projectId: normalizeOptionalId(input.projectId),
    projectTitle: normalizeOptionalText(input.projectTitle),
    workspaceRoot: normalizeOptionalText(input.workspaceRoot),
    threadId: normalizeOptionalId(input.threadId),
    threadTitle: normalizeOptionalText(input.threadTitle),
    updatedAt: normalizeOptionalText(input.updatedAt) ?? new Date().toISOString(),
  };
}

export const DesktopContextLive = Layer.effect(
  DesktopContext,
  Effect.gen(function* () {
    const state = yield* Ref.make<ServerDesktopContext>(emptyDesktopContext);
    const pubsub = yield* PubSub.unbounded<ServerDesktopContext>();

    const set: DesktopContextShape["set"] = (input) =>
      Effect.gen(function* () {
        const next = normalizeDesktopContext(input);
        yield* Ref.set(state, next);
        yield* PubSub.publish(pubsub, next);
        return next;
      });

    return {
      get: Ref.get(state),
      set,
      get stream() {
        return Stream.fromPubSub(pubsub);
      },
    } satisfies DesktopContextShape;
  }),
);
