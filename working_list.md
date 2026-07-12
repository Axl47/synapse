# Working List

## In Progress

## Pending

## Done

- [x] Trace static and runtime Codex model discovery - verified that current local and upstream catalogs omit GPT-5.6 while runtime parsing already accepts new effort values
- [x] Define implementation boundary in `.docs/exec/gpt-5-6-models-and-ultra-effort.html`
- [x] Add GPT-5.6 Sol, Terra, and Luna with complete reasoning-effort capabilities - shared model tests pass (77 tests)
- [x] Add an Ultra-specific accent to picker rows and the compact composer label - centralized presentation helper is used by both surfaces
- [x] Add focused contract, normalization, and UI coverage - model, runtime metadata, app settings, server fallback, and browser selector tests added or updated
- [x] Run focused tests and review the final diff - 252 focused tests passed, including 23 Chromium component tests; `git diff --check` passed
- [x] Compare the fresh-draft and started-thread composer paths - confirmed fresh drafts alone still used the legacy split model/effort controls
- [x] Preserve Max and Ultra through stale app-server runtime metadata - dispatch-state coverage confirms Ultra remains selected; Terra rejects Sol-only Ultra
- [x] Unify fresh-draft and started-thread composer controls - the combined model/effort picker is now the only composer path
- [x] Verify the follow-up - 78 focused web unit tests, 63 ChatView Chromium tests, and 23 traits Chromium tests passed
