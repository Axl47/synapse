# Working List

## Pending

- [ ] Phase 3: implement server-owned idempotent adoption and shared import operations
- [ ] Phase 4: implement project-local sidebar, unmatched tasks, Search integration, and adoption UX
- [ ] Phase 5: implement idempotent persisted-history reconciliation
- [ ] Phase 6: harden, document, and run the final required verification pass
- [ ] Audit every ExecPlan outcome and close implementation

## In Progress

- [~] Phase 2: implement multi-account discovery, deduplication, and project/worktree matching

## Done

- [x] Phase 1: implement the Codex thread/list protocol boundary and adapter coverage (136 focused server tests passed)
- [x] Validate the external Codex thread discovery ExecPlan structure, required sections, HTML element balance, and whitespace
- [x] Write the standalone HTML ExecPlan for discovery, project matching, adoption, and reconciliation
- [x] Trace the existing Codex import, thread/read, provider-instance, RPC, and sidebar project paths
- [x] Confirm the integrated branch contains a combined picker component but only uses it after a thread starts
- [x] Confirm favourites are currently nested inside individual provider model lists
- [x] Confirm persisted Codex effort hydration rejects Max and Ultra and falls back to Medium
- [x] Make the combined model/effort picker the only composer picker path
- [x] Add a top-level Favourites model section across providers and instances
- [x] Preserve Codex Max and Ultra through draft hydration and provider dispatch
- [x] Restore the GPT-5.6 built-in fallback capabilities and Ultra styling
- [x] Add focused regression coverage
- [x] Verify 227 web unit tests, 46 browser tests, 78 shared tests, and 136 server tests
- [x] Commit and publish the verified corrections to `multiple-accounts-selection`
- [x] Remove account/default labels from global favourite rows
- [x] Remove duplicate provider-local Favorites grouping
- [x] Make the composer account trigger icon-only
- [x] Verify 29 picker browser tests and 9 grouping unit tests
- [x] Label clean publish-only Git actions as Push
- [x] Verify all 82 Git actions logic tests
- [x] Label GPT-5.6 low reasoning as Light while preserving the `low` protocol value
- [x] Verify 27 composer capability tests and 79 shared model tests
