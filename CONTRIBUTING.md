# Contributing

We treat this repo as "Open Source" within Redis: anyone who clears the bar below is welcome to contribute.

## Local setup

```bash
git clone git@github.com:redis-performance/geo-bench.git
cd geo-bench

# Download module dependencies
GO111MODULE=on go get -t -v ./...

# Build the binary
make build
# The geo-bench binary is created in the current directory
```

Go 1.19+ is required. The build uses `CGO_ENABLED=0` so no C toolchain is needed.

## Branch naming

```
<type>/<short-description>
```

Types: `feat`, `fix`, `refactor`, `test`, `docs`, `chore`

Example: `feat/add-pipeline-mode`

## Coding standards

- Keep changes focused; one logical change per PR.
- Follow the conventions already present in the codebase (formatting, naming, error handling).
- No dead code, no commented-out blocks.

## Submitting changes

1. Fork or create a branch from `main`.
2. Make your changes with clear, atomic commits.
3. Open a pull request against `main` with a descriptive title and summary.
4. Address review comments promptly; force-push to the same branch to update.

## Testing

- All new behaviour must be covered by tests.
- Existing tests must pass: run the test suite locally before opening a PR.
- Coverage should not decrease.

Run the full test suite (requires a Redis instance on localhost:6379):

```bash
make test
```

`make test` runs `gofmt` over all packages and then executes `go test -race -covermode=atomic ./...`. CI also exercises integration tests against a live Redis service.

## Review process

- At least one maintainer approval is required before merge.
- CI must be green.
- Maintainers may request changes or close PRs that do not meet the bar -- this is normal and not personal.
