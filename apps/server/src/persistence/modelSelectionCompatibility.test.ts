// FILE: modelSelectionCompatibility.test.ts
// Purpose: Protects provider inference and option normalization for persisted model selections.
// Layer: Persistence compatibility tests
// Depends on: modelSelectionCompatibility.

import { assert, it } from "@effect/vitest";
import { DEFAULT_SERVER_SETTINGS } from "@synara/contracts";

import { normalizePersistedModelSelection } from "./modelSelectionCompatibility.ts";

it("preserves canonical Pi model selections", () => {
  assert.deepEqual(
    normalizePersistedModelSelection({ instanceId: "pi", model: "openai/gpt-5.5" }),
    {
      instanceId: "pi",
      model: "openai/gpt-5.5",
    },
  );
});

it("migrates combined Antigravity model and effort labels", () => {
  assert.deepEqual(
    normalizePersistedModelSelection({
      provider: "antigravity",
      model: "Gemini 3.5 Flash (High)",
    }),
    {
      instanceId: "antigravity",
      model: "Gemini 3.5 Flash",
      options: [{ id: "reasoningEffort", value: "high" }],
    },
  );
});

it("infers Antigravity from persisted instance labels", () => {
  assert.deepEqual(
    normalizePersistedModelSelection({
      instanceId: "Antigravity CLI",
      model: "Claude Sonnet 4.6 (Thinking)",
    }),
    {
      instanceId: "Antigravity CLI",
      model: "Claude Sonnet 4.6",
      options: [{ id: "reasoningEffort", value: "thinking" }],
    },
  );
});

it("prefers an explicit Antigravity instance over a model vendor in its label", () => {
  assert.deepEqual(
    normalizePersistedModelSelection({
      instanceId: "Antigravity Claude runtime",
      model: "Claude Sonnet 4.6 (Thinking)",
    }),
    {
      instanceId: "Antigravity Claude runtime",
      model: "Claude Sonnet 4.6",
      options: [{ id: "reasoningEffort", value: "thinking" }],
    },
  );
});

it("migrates known Gemini models without discarding the saved selection", () => {
  assert.deepEqual(
    normalizePersistedModelSelection({
      provider: "gemini",
      model: "gemini-3.1-pro-preview",
    }),
    {
      instanceId: "antigravity",
      model: "Gemini 3.1 Pro",
    },
  );
});

it("preserves unknown Gemini models as custom Antigravity selections", () => {
  assert.deepEqual(
    normalizePersistedModelSelection({
      provider: "gemini",
      model: "gemini-custom-preview",
    }),
    {
      instanceId: "antigravity",
      model: "gemini-custom-preview",
    },
  );
});

it("infers Pi from persisted instance labels", () => {
  assert.deepEqual(
    normalizePersistedModelSelection({
      instanceId: "local-pi-runtime-instance",
      model: "openai/gpt-5.5",
    }),
    {
      instanceId: "local-pi-runtime-instance",
      model: "openai/gpt-5.5",
    },
  );
});

it("preserves provider instance ids from providerless persisted selections", () => {
  assert.deepEqual(
    normalizePersistedModelSelection({
      instanceId: "claude_work",
      model: "claude-sonnet-4-6",
    }),
    {
      instanceId: "claude_work",
      model: "claude-sonnet-4-6",
    },
  );
});

it("normalizes explicit empty legacy model options", () => {
  assert.deepEqual(
    normalizePersistedModelSelection({
      provider: "codex",
      model: "gpt-5",
      options: {},
    }),
    {
      instanceId: "codex",
      model: "gpt-5",
      options: [],
    },
  );
});

it("infers Claude from providerless Sonnet instance selections", () => {
  assert.deepEqual(
    normalizePersistedModelSelection({
      instanceId: "work",
      model: "sonnet-4",
    }),
    {
      instanceId: "work",
      model: "sonnet-4",
    },
  );
});

it("infers OpenCode from providerless OpenCode model selections", () => {
  assert.deepEqual(
    normalizePersistedModelSelection({
      instanceId: "work",
      model: "opencode/minimax-m2.5-free",
    }),
    {
      instanceId: "work",
      model: "opencode/minimax-m2.5-free",
    },
  );
});

it("leaves ambiguous providerless instance selections untouched without settings", () => {
  assert.deepEqual(
    normalizePersistedModelSelection({
      instanceId: "work",
      model: "custom-model",
    }),
    {
      instanceId: "work",
      model: "custom-model",
    },
  );
});

it("resolves ambiguous providerless instance selections from settings", () => {
  assert.deepEqual(
    normalizePersistedModelSelection(
      {
        instanceId: "work",
        model: "custom-model",
      },
      {
        ...DEFAULT_SERVER_SETTINGS,
        providerInstances: {
          work: {
            driver: "claudeAgent",
            enabled: true,
            config: { homePath: "/tmp/claude-work" },
          },
        },
      },
    ),
    {
      instanceId: "work",
      model: "custom-model",
    },
  );
});

it("infers Droid only for Factory-exclusive provider-less model slugs", () => {
  assert.deepEqual(normalizePersistedModelSelection({ model: "minimax-m3" }), {
    instanceId: "droid",
    model: "minimax-m3",
  });
});

it("does not attribute an ambiguous Claude slug to Droid", () => {
  assert.deepEqual(normalizePersistedModelSelection({ model: "claude-opus-4-8" }), {
    instanceId: "claudeAgent",
    model: "claude-opus-4-8",
  });
});
