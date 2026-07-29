# Working List

## Current Task: Pragma Prompt Workspace — Synara Draft Handoff

## In Progress

- None.

## Pending

- [ ] Cross-repository integration — validate the packaged Pragma-to-Synara path
- [ ] Final authorized validation — run focused suites plus `bun fmt`, `bun lint`, and `bun typecheck`

## Blocked

- None.

## Done

- [x] Milestone 5 — define and persist external composer-draft imports
  - [x] Reconciled current contract, persistence, HTTP, migration, and test seams
  - [x] Added versioned contracts and Effect RPC methods
  - [x] Added migration 096 and repository/domain lifecycle
  - [x] Added authenticated bounded upload/download routes and cleanup/recovery
  - [x] Passed contract, migration, repository, HTTP, and server build checks
- [x] Milestone 6 — hydrate an imported package into the ordinary standalone composer
  - [x] Added exact preset validation and claimed attachment checksum verification
  - [x] Registered imported drafts without replacing project draft mappings
  - [x] Generalized IndexedDB persistence and reload hydration to ordinary files
  - [x] Re-read local storage and blob bytes before durable completion acknowledgement
  - [x] Mounted a coordinator that navigates without dispatching a provider turn
  - [x] Passed 89 focused web tests, HTTP tests, and the production web build
- [x] Milestone 7 — add reliable desktop activation and navigation
  - [x] Published the loopback HTTP origin, flavor-specific activation base, and capability in descriptor v2
  - [x] Added strict opaque activation parsing and a bounded FIFO for cold and warm activation
  - [x] Wired macOS open-url, Windows/Linux second-instance arguments, cold argv, focus, and packaged protocol registration
  - [x] Buffered the typed desktop-to-web intent until the root coordinator subscribes
  - [x] Passed 16 desktop activation/descriptor tests, 33 server config tests, the hydration test, and desktop/server/web production builds

## Previous Task: Upstream Sync

### In Progress

- None.

### Pending

- None.

### Done

- [x] Confirmed a clean `main` at `origin/main` (`19ab2b5f`)
- [x] Fetched `origin` and `upstream` separately
- [x] Measured divergence: fork 198 commits, upstream 29 commits
- [x] Preflighted the merge and recorded the exact conflict surface
- [x] Created the upstream-sync ExecPlan
- [x] Resolved desktop and provider lifecycle/runtime-mode conflicts
- [x] Resolved orchestration, persistence, migration, and contract conflicts
- [x] Resolved web composer, model/account picker, Git actions, automation, and terminal conflicts
- [x] Preserved provider/account isolation, external Codex adoption, and upstream recovery semantics
- [x] Freeze-checked `bun.lock` with `bun install --frozen-lockfile`
- [x] Passed focused integration tests and the complete 3,514-test server suite
- [x] Passed the 3,323-test web baseline plus corrected focused compiler/prefetch checks
- [x] Passed the production web build and all root package suites outside the server
- [x] Verified whitespace, conflict-marker removal, upstream ancestry, and the merge tree
- [x] Prepared `main` for push to `origin` and remote-tip confirmation
- [x] Left `bun fmt`, `bun lint`, and `bun typecheck` unrun because repository policy requires explicit authorization
