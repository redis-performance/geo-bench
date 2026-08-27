# Recurring bug taxonomy — geo-bench, real precedent only

Grounded in this repo's own self-authored, merged bugfix commits and its actual source layout (`gh pr diff`
against PRs #16, #19, #20, #21, #25, #26; `go.mod`; `cmd/redis/redis.go`; `cmd/root.go`), surveyed August 2026.
As `references/review-history.md` explains, none of these are reviewer catches — this repo has no history of
substantive reviewer comments at all. Every item below is a real bug this codebase's own author found and fixed
in themselves, which is still genuine, on-point precedent for what actually goes wrong in this specific code,
just with different provenance than a reviewer quote. Say so if you cite one — these are "this codebase has
broken this way before," not "a reviewer flagged this before."

1. **A CLI flag's string-literal name can drift from the shared constant that defines it.** PR#16's real fix:
   `cmd/query.go` looked up the `uri` flag via the bare string literal `pflags.GetString("uri")`, while the flag
   itself (and every other call site) referenced it via `redis.REDIS_URI_PROPERTY`, a constant defined in
   `cmd/redis/redis.go` (e.g. `REDIS_URI_PROPERTY`, `REDIS_GEO_KEYNAME_PROPERTY`, `REDIS_IDX_PROPERTY` are all
   defined there as named constants precisely to avoid this). When a PR reads or sets a `pflag` by name, check
   whether it uses the shared constant or a bare string literal that could silently diverge from it.

2. **Adding a new parameter to a worker/setup function means threading it through every call site, not just the
   most visible one.** PR#21's real diff added a `connWriteTimeout` value and had to pass it through
   `setupStageGeoShape`, `setupStageGeoPoint`, `loadWorkerGeoshape`, and `loadWorkerGeopoint` — four separate
   call sites across `cmd/load.go`. This codebase's worker functions tend to take long lists of positional,
   same-typed arguments (URIs, passwords, db names, index names, counters), which makes it easy to update the
   signature but miss a call site, or to pass two same-typed arguments in the wrong order without the compiler
   catching it. Trace every call site of a changed function signature by hand, and look twice at any two adjacent
   parameters of the same type.

3. **This tool has two parallel backend implementations (`cmd/redis/` and `cmd/elasticsearch/`) that exist
   specifically to be compared against each other** (the README's own stated purpose: "compare the performance
   of different Redis configurations... and RediSearch... Elasticsearch"). A behavioral change to one backend's
   load/query logic that isn't mirrored in the other can silently turn an intended apples-to-apples comparison
   into an apples-to-oranges one. When a PR changes timing, retry, connection, or query-construction behavior in
   one backend, check whether the equivalent path in the other backend needs the same treatment — or whether the
   PR description explains why it doesn't.

4. **Unpinned or under-specified CI versions have caused real, self-authored breakage here twice in a row.**
   PR#25 fixed the Go version matrix (`1.18.x, 1.19.x` → `1.20.x, 1.21.x`) after a dependency (`rueidis`) required
   Go 1.20+; PR#26, one day later, pinned the CI Redis service container from a floating `redis` tag to
   `redis:8.6` after presumably being bitten by an unpinned image drifting to an incompatible version. Both are
   real, recent (May 2026), self-authored fixes to `.github/workflows/test.yml`. Treat any PR that adds or
   changes a version constraint, a Docker image tag, or a language-version matrix entry in this repo's CI as
   worth double-checking against the actual dependency requirements in `go.mod`, not just copy-pasted from
   elsewhere.

5. **`go.mod` currently points `github.com/redis/rueidis` at a personal fork** (`replace github.com/redis/rueidis
   => github.com/filipecosta90/rueidis v0.0.0-...`), not the upstream module. This is a real, present state of
   this codebase, not a hypothetical — worth naming (not necessarily blocking) if a PR touches `go.mod`/`go.sum`,
   since it means a routine-looking dependency bump could unintentionally revert onto the personal fork's
   history, or vice versa, and it's worth confirming the PR author understands which one they intended to target.

## What this taxonomy is honestly thin or silent on

- **Everything about review dialogue, disagreement, or a maintainer requesting changes.** There is no real
  example anywhere in this repo's history (see `review-history.md`) — don't invent one.
- **Test coverage enforcement mechanics.** `CONTRIBUTING.md` states test coverage should not decrease, but there
  is no coverage-reporting bot (no Codecov, no equivalent) wired into `.github/workflows/test.yml` — CI only runs
  `go test -race -covermode=atomic ./...`, which computes coverage but does not gate on it or post it anywhere.
  Treat the written rule as real but manually-enforced-if-at-all, not mechanically checked.
- **Elasticsearch-specific correctness bugs.** The real, mined bugfix history (PRs #16, #19, #20, #21, #25, #26)
  skews toward the Redis backend and CI; no Elasticsearch-specific bugfix commit turned up in this survey. Don't
  assume equal real precedent for both backends — the dual-backend concern in item 3 is a structural inference
  from the codebase's stated purpose, not a citation of a real Elasticsearch bug that happened here.
