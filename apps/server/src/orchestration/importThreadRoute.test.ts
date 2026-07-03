import { homedir } from "node:os";
import path from "node:path";

import type { ProviderStartOptions } from "@t3tools/contracts";
import { describe, expect, it } from "vitest";

import { claudeHistoricalSessionEnvironment } from "./importThreadRoute";

describe("claudeHistoricalSessionEnvironment", () => {
  it("expands instance Claude homes the same way session launches do", () => {
    const environment = claudeHistoricalSessionEnvironment({
      claudeAgent: {
        homePath: "~/claude-work",
        environment: { SYNARA_CLAUDE_IMPORT_TEST: "1" },
      },
    } satisfies ProviderStartOptions);

    expect(environment?.HOME).toBe(path.join(homedir(), "claude-work"));
    expect(environment?.SYNARA_CLAUDE_IMPORT_TEST).toBe("1");
  });
});
