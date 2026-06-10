# Working List

## Pending

## In Progress

## Done
- [x] Create working checklist for deeper performance investigation — added this file.
- [x] Trace terminal tab close lifecycle and synchronous work — confirmed synchronous xterm/WebGL disposal before tab state update and state-driven disposal as likely close hang sources.
- [x] Trace remaining composer input lag paths — confirmed prompt persistence was still running through Zustand on every keystroke.
- [x] Implement confirmed fixes with narrow scope — debounced prompt draft persistence and deferred terminal runtime disposal.
- [x] Run focused verification and summarize residual risk — `bun run --cwd apps/web test src/composerDraftStore.test.ts src/terminalStateStore.test.ts src/lib/terminalCloseConfirmation.test.ts src/lib/terminalStateCleanup.test.ts src/components/terminal/terminalRuntimeTypes.test.ts` and `git diff --check` pass.
