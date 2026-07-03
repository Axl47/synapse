// FILE: PiAdapter.test.ts
// Purpose: Verifies Pi adapter model discovery exposes only SDK-supported thinking levels.
// Layer: Provider adapter tests
// Depends on: PiAdapter discovery helpers and Pi model metadata shapes.

import type { AuthStorage, ModelRegistry } from "@earendil-works/pi-coding-agent";
import type { Api, Model } from "@earendil-works/pi-ai";
import { describe, expect, it, vi } from "vitest";
import {
  applyPiRuntimeApiKeysFromEnvironment,
  createPiModelRegistry,
  getPiSupportedThinkingOptions,
  makePiUserInputOptions,
  PLAIN_PI_EXTENSION_THEME,
} from "./PiAdapter";

function makePiModel(input: {
  reasoning: boolean;
  thinkingLevelMap?: Model<Api>["thinkingLevelMap"];
}): Pick<Model<Api>, "reasoning" | "thinkingLevelMap"> {
  return {
    reasoning: input.reasoning,
    ...(input.thinkingLevelMap !== undefined ? { thinkingLevelMap: input.thinkingLevelMap } : {}),
  };
}

describe("getPiSupportedThinkingOptions", () => {
  it("hides thinking controls for non-reasoning models", () => {
    expect(getPiSupportedThinkingOptions(makePiModel({ reasoning: false }))).toEqual([]);
  });

  it("advertises xhigh only when the concrete Pi model supports it", () => {
    const withoutXHigh = getPiSupportedThinkingOptions(makePiModel({ reasoning: true }));
    const withXHigh = getPiSupportedThinkingOptions(
      makePiModel({ reasoning: true, thinkingLevelMap: { xhigh: "xhigh" } }),
    );

    expect(withoutXHigh.map((option) => option.value)).toEqual([
      "off",
      "minimal",
      "low",
      "medium",
      "high",
    ]);
    expect(withXHigh.map((option) => option.value)).toEqual([
      "off",
      "minimal",
      "low",
      "medium",
      "high",
      "xhigh",
    ]);
  });

  it("respects provider-level disabled thinking levels", () => {
    const options = getPiSupportedThinkingOptions(
      makePiModel({
        reasoning: true,
        thinkingLevelMap: {
          off: null,
          minimal: "low",
          low: "low",
          medium: "medium",
          high: "high",
        },
      }),
    );

    expect(options.map((option) => option.value)).toEqual(["minimal", "low", "medium", "high"]);
  });
});

describe("applyPiRuntimeApiKeysFromEnvironment", () => {
  it("uses the same runtime auth storage for API keys and model registry", () => {
    const authStorage = {
      setRuntimeApiKey: vi.fn(),
    } as unknown as AuthStorage;
    const registry = {} as ModelRegistry;
    const piSdk = {
      AuthStorage: {
        create: vi.fn(() => authStorage),
      },
      ModelRegistry: {
        create: vi.fn(() => registry),
      },
    } as unknown as Parameters<typeof createPiModelRegistry>[1];

    const context = createPiModelRegistry("/agent", piSdk, {
      OPENAI_API_KEY: "instance-openai-key",
    });

    expect(piSdk.AuthStorage.create).toHaveBeenCalledWith("/agent/auth.json");
    expect(authStorage.setRuntimeApiKey).toHaveBeenCalledWith("openai", "instance-openai-key");
    expect(piSdk.ModelRegistry.create).toHaveBeenCalledWith(authStorage, "/agent/models.json");
    expect(context.authStorage).toBe(authStorage);
    expect(context.registry).toBe(registry);
  });

  it("maps Pi provider-instance API keys into runtime auth without mutating process.env", () => {
    const previousOpenAiKey = process.env.OPENAI_API_KEY;
    process.env.OPENAI_API_KEY = "global-openai-key";
    const runtimeKeys = new Map<string, string>();

    try {
      applyPiRuntimeApiKeysFromEnvironment(
        {
          setRuntimeApiKey(provider, apiKey) {
            runtimeKeys.set(provider, apiKey);
          },
        },
        {
          OPENAI_API_KEY: "instance-openai-key",
          ANTHROPIC_API_KEY: "anthropic-api-key",
          ANTHROPIC_OAUTH_TOKEN: "anthropic-oauth-token",
          OPENCODE_API_KEY: "opencode-key",
        },
      );
    } finally {
      if (previousOpenAiKey === undefined) {
        delete process.env.OPENAI_API_KEY;
      } else {
        process.env.OPENAI_API_KEY = previousOpenAiKey;
      }
    }

    expect(runtimeKeys.get("openai")).toBe("instance-openai-key");
    expect(runtimeKeys.get("anthropic")).toBe("anthropic-oauth-token");
    expect(runtimeKeys.get("opencode")).toBe("opencode-key");
    expect(runtimeKeys.get("opencode-go")).toBe("opencode-key");
    expect(process.env.OPENAI_API_KEY).toBe(previousOpenAiKey);
  });
});

describe("Pi extension UI helpers", () => {
  it("keeps original select values while showing normalized unique labels", () => {
    const mappings = makePiUserInputOptions(["  OpenRouter  ", "", "OpenRouter"]);

    expect(mappings.map((mapping) => mapping.value)).toEqual(["  OpenRouter  ", "", "OpenRouter"]);
    expect(mappings.map((mapping) => mapping.option.label)).toEqual([
      "OpenRouter",
      "Option 2",
      "OpenRouter (2)",
    ]);
  });

  it("provides a no-color theme object for UI-gated extensions", () => {
    expect(PLAIN_PI_EXTENSION_THEME.fg("accent", "ready")).toBe("ready");
    expect(PLAIN_PI_EXTENSION_THEME.bold("done")).toBe("done");
    expect(PLAIN_PI_EXTENSION_THEME.getThinkingBorderColor("medium")("thinking")).toBe("thinking");
  });
});
