# Real review history — redis-performance/geo-bench

Mined via `gh pr list --repo redis-performance/geo-bench --state all --limit 300`, `gh api
repos/redis-performance/geo-bench/pulls/<n>/reviews`, `gh api
repos/redis-performance/geo-bench/issues/<n>/comments`, and `gh api repos/redis-performance/geo-bench/contributors`
in August 2026, covering the repo's entire history (26 real PRs, 2022–2026; 5 of the low-numbered PRs were bot
labeling tests, opened and closed same-day by the repo owner, and carry no review content).

**The honest headline: there is close to no review culture here to describe, and this file exists to say that
plainly rather than paper over it.**

- **Two contributors, one person.** `gh api .../contributors` shows exactly two logins: `filipecosta90` (37
  commits) and `fcostaoliveira` (9 commits). Both are Filipe Oliveira — the PR list's `author.name` field shows
  "Filipe Oliveira (Personal)" for the first identity and "Filipe Oliveira (Redis)" for the second. Every single
  merged PR in this repo's history was authored by this one person.
- **Zero issues, ever.** `gh issue list --repo redis-performance/geo-bench --state all` returns an empty list.
  There is no issue-triage history to mine at all.
- **Zero substantive PR comments, ever.** Every `issues/<n>/comments` call against a sample spanning the repo's
  full date range (PRs 12, 14, 16, 19, 20, 21, 22, 23, 25, 26) returned an empty array. No one has ever left a
  written PR comment on this repo, positive or negative.
- **The only reviewer besides the author is `paulorsousa`, and his reviews carry no text.** On the three most
  recent PRs (#22, #25, #26 — all from May 2026), `paulorsousa` (a repo MEMBER) left a formal GitHub `APPROVED`
  review, but the `body` field on every one of those three reviews is the empty string `""`. All older PRs
  (2022–2023) show zero reviews of any kind — they were merged by the author with no reviewer at all. There is
  no real example anywhere in this repo's history of `paulorsousa` (or anyone) writing so much as one sentence
  of review feedback.
- **PR turnaround is same-day to same-minute** across the whole history — several PRs were opened and merged
  within seconds to minutes of each other, consistent with a single person merging their own work.

**What this means for using this skill:** there is no "voice" to imitate, because no one in this repo's real
history has ever written a review comment. Resist the pull to manufacture one — e.g., don't invent a
`paulorsousa` review style from three empty-body approvals; there is nothing there to characterize beyond "he
clicks approve." The only real, citable institutional standards this repo has are its own written `AGENTS.md`
and `CONTRIBUTING.md` (both added in PR#22, one of the three PRs with a real — if textless — review), and the
repo's own self-authored bugfix commits, catalogued in `references/bugfix-taxonomy.md`. Ground every review in
those, and say plainly, if it comes up, that this repo's history doesn't support a richer review-culture citation
than that.

## If this changes

If a future PR on this repo actually receives a substantive written review comment, that would be new, real
precedent worth folding into this file — don't let this file's current "there is nothing here" framing calcify
into permanent doctrine if the record changes.
