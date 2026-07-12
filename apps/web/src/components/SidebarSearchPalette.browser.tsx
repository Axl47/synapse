// Production breakpoints are part of this regression: import targets must stay
// readable when several provider accounts share a narrow command palette.
import "../index.css";

import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { page } from "vitest/browser";
import { describe, expect, it, vi } from "vitest";
import { render } from "vitest-browser-react";

import type { ExternalThreadCandidate } from "@synara/contracts";
import type { ThreadImportTarget } from "../lib/threadImport";
import { SidebarSearchPalette } from "./SidebarSearchPalette";

const MANY_IMPORT_TARGETS = [
  { provider: "codex", instanceId: "codex", label: "Personal Codex" },
  { provider: "codex", instanceId: "codex_work", label: "Work Codex" },
  { provider: "codex", instanceId: "codex_client", label: "Client Codex" },
  { provider: "claudeAgent", instanceId: "claudeAgent", label: "Personal Claude" },
  { provider: "claudeAgent", instanceId: "claude_work", label: "Work Claude" },
  { provider: "cursor", instanceId: "cursor", label: "Default Cursor" },
  { provider: "kilo", instanceId: "kilo", label: "Default Kilo" },
  { provider: "opencode", instanceId: "opencode_work", label: "Work OpenCode" },
] as const satisfies ReadonlyArray<ThreadImportTarget>;

const IMPORT_TARGET_VIEWPORTS = [
  { width: 320, height: 700, expectedColumns: 1 },
  { width: 800, height: 700, expectedColumns: 2 },
] as const;

describe("SidebarSearchPalette import targets", () => {
  for (const viewport of IMPORT_TARGET_VIEWPORTS) {
    it(`keeps many account identities usable at ${viewport.width}px`, async () => {
      await page.viewport(viewport.width, viewport.height);
      const queryClient = new QueryClient({
        defaultOptions: { queries: { retry: false } },
      });
      const screen = await render(
        <QueryClientProvider client={queryClient}>
          <SidebarSearchPalette
            open
            mode="import"
            onModeChange={vi.fn()}
            onOpenChange={vi.fn()}
            actions={[]}
            projects={[]}
            threads={[]}
            onCreateChat={vi.fn()}
            onCreateThread={vi.fn()}
            onAddProjectPath={async () => {}}
            homeDir={null}
            onOpenSettings={vi.fn()}
            onOpenUsageSettings={vi.fn()}
            onOpenProject={vi.fn()}
            onOpenThread={vi.fn()}
            importTargets={MANY_IMPORT_TARGETS}
            onImportThread={vi.fn()}
          />
        </QueryClientProvider>,
      );

      try {
        const targetGroup = page.getByRole("radiogroup", { name: "Provider account" });
        await expect.element(targetGroup).toBeInTheDocument();
        expect(page.getByRole("radio").length).toBe(MANY_IMPORT_TARGETS.length);
        for (const label of ["Personal Codex", "Work Claude", "Work OpenCode"]) {
          await expect.element(page.getByText(label, { exact: true })).toBeInTheDocument();
        }

        const groupElement = targetGroup.element();
        const groupRect = groupElement.getBoundingClientRect();
        const gridTrackWidths = getComputedStyle(groupElement)
          .gridTemplateColumns.split(" ")
          .map((track) => Number.parseFloat(track));
        expect(gridTrackWidths).toHaveLength(viewport.expectedColumns);
        expect(groupElement.scrollWidth).toBeLessThanOrEqual(groupElement.clientWidth + 1);
        expect(groupElement.scrollHeight).toBeGreaterThan(groupElement.clientHeight);
        const options = groupElement.querySelectorAll<HTMLElement>("[role='radio']");
        for (const [index, option] of Array.from(options).entries()) {
          const optionRect = option.getBoundingClientRect();
          const trackWidth = gridTrackWidths[index % viewport.expectedColumns];
          if (trackWidth === undefined) {
            throw new Error("Missing computed import-target grid track");
          }
          expect(Math.abs(optionRect.width - trackWidth)).toBeLessThanOrEqual(1);
          expect(optionRect.height).toBeGreaterThanOrEqual(44);
          expect(optionRect.left).toBeGreaterThanOrEqual(groupRect.left - 1);
          expect(optionRect.right).toBeLessThanOrEqual(groupRect.right + 1);
        }

        const workCodex = page.getByRole("radio", { name: /Work Codex.*Codex/ });
        const workOpenCode = page.getByRole("radio", { name: /Work OpenCode.*OpenCode/ });
        await workCodex.click();
        await expect.element(workCodex).toHaveAttribute("aria-checked", "true");
        await workOpenCode.click();
        await expect.element(workOpenCode).toHaveAttribute("aria-checked", "true");
        await expect.element(workCodex).toHaveAttribute("aria-checked", "false");
      } finally {
        await screen.unmount();
        queryClient.clear();
        await page.viewport(1280, 720);
      }
    });
  }
});

describe("SidebarSearchPalette external threads", () => {
  it("labels an external Codex result and opens it through adoption", async () => {
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    const onOpenExternalThread = vi.fn();
    const candidate = {
      provider: "codex",
      providerInstanceId: "codex",
      externalThreadId: "external-1",
      title: "Investigate reconnect",
      preview: "Trace the reconnect path",
      cwd: "/repo/synara",
      sourceKind: "appServer",
      status: "idle",
      modelProvider: "openai",
      createdAt: "2026-07-12T12:00:00.000Z",
      updatedAt: "2026-07-12T12:05:00.000Z",
      matchedProjectId: "project-1",
      matchKind: "exact",
    } as const satisfies ExternalThreadCandidate;
    const screen = await render(
      <QueryClientProvider client={queryClient}>
        <SidebarSearchPalette
          open
          mode="search"
          onModeChange={vi.fn()}
          onOpenChange={vi.fn()}
          actions={[]}
          projects={[]}
          threads={[
            {
              id: "external:codex:external-1",
              title: candidate.title,
              projectId: "project-1",
              projectName: "Synara",
              projectRemoteName: "Synara",
              provider: "codex",
              createdAt: candidate.createdAt,
              updatedAt: candidate.updatedAt,
              messages: [{ text: candidate.preview }],
              externalThread: candidate,
            },
          ]}
          onCreateChat={vi.fn()}
          onCreateThread={vi.fn()}
          onAddProjectPath={async () => {}}
          homeDir={null}
          onOpenSettings={vi.fn()}
          onOpenUsageSettings={vi.fn()}
          onOpenProject={vi.fn()}
          onOpenThread={vi.fn()}
          importTargets={[]}
          onImportThread={vi.fn()}
          onOpenExternalThread={onOpenExternalThread}
        />
      </QueryClientProvider>,
    );

    try {
      await expect.element(page.getByText("Investigate reconnect", { exact: true })).toBeVisible();
      await expect.element(page.getByText("External · Codex App", { exact: true })).toBeVisible();
      await page.getByText("Investigate reconnect", { exact: true }).click();
      expect(onOpenExternalThread).toHaveBeenCalledWith(candidate);
    } finally {
      await screen.unmount();
      queryClient.clear();
    }
  });
});
