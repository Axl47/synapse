# Working List

## Pending
- [ ] Summarize final sync state and manual verification checklist

## In Progress

## Done
- [x] Audit branch/remotes and local-vs-upstream divergence - current branch is `UI-changes`, clean aside from this checklist; upstream is 37 commits ahead of this branch and the dry merge predicts conflicts in `ChatHeader.tsx` and `Migrations.ts`.
- [x] Merge `upstream/main` into `UI-changes` - resolved the predicted conflicts and staged the merge result.
- [x] Preserve local run actions and fork-specific UI/workflow affordances - kept `ProjectScriptsControl` mounted inline in `ChatHeader` without `showInlineControls={false}`, while adopting upstream's temporary-thread lifecycle changes.
- [x] Resolve migration/contract conflicts without dropping upstream behavior - kept upstream `049_ProjectionThreadMessagesDispatchOrigin` and renumbered the fork schema-drift repair to `050_ReconcileProjectionSchemaDrift`.
- [x] Run focused verification allowed by project constraints - `git diff --cached --check`, server migration tests, web header/git action tests, and contracts websocket/git tests passed; full `bun fmt`, `bun lint`, and `bun typecheck` were not run because this repo requires explicit user request for those heavyweight checks.
