# Working List

## Pending
- [ ] Summarize final sync state and manual verification checklist

## In Progress

## Done
- [x] Audit branch/remotes and local-vs-upstream divergence - current branch is clean `main`, matching `origin/main`; after fetch, `upstream/main` is 22 commits ahead while local `main` has 28 fork commits to preserve. Dry merge predicts conflicts in `apps/server/src/persistence/Migrations/044_Automations.test.ts` and `apps/web/src/routes/__root.tsx`.
- [x] Merge `upstream/main` into `main` - resolved the predicted conflicts and staged the merge result.
- [x] Preserve local run actions and fork-specific workflow affordances - verified `ProjectScriptsControl` remains mounted in `ChatHeader`, migration `049`/`050` ordering remains intact, and desktop-context websocket contracts remain alongside upstream automation/server contracts.
- [x] Resolve any migration/contract conflicts without dropping upstream behavior - accepted upstream's migration-test comment/lookup and mounted both `DesktopContextReporter` and `ProviderStatusRefreshCoordinator` in the root route.
- [x] Run focused verification allowed by project constraints - `git diff --cached --check`, server migration tests, web query/header/git/ws tests, and contracts websocket/git/automation tests passed; full `bun fmt`, `bun lint`, and `bun typecheck` were not run because this repo requires explicit user request for those heavyweight checks.
