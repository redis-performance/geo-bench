# Agent guidelines

Instructions for AI coding agents (Claude Code, Copilot, Cursor, etc.) working in this repo.

## Project overview

`geo-bench` is a Go CLI tool for benchmarking Redis GEO commands and RediSearch. It loads geo-point and geo-polygon datasets into Redis (or Elasticsearch) and then drives configurable query workloads, reporting throughput and latency histograms. The two main sub-commands are `load` (ingest a JSON dataset) and `query` (run timed GEO queries). The tool is a single Go module (`filipecosta90/geo-bench`) built with `cobra` for CLI parsing and `rueidis` as the Redis client.

## Local setup

```bash
git clone git@github.com:redis-performance/geo-bench.git
cd geo-bench

# Download module dependencies
GO111MODULE=on go get -t -v ./...

# Build the binary (CGO_ENABLED=0, no C toolchain needed)
make build
# produces ./geo-bench

# Quick smoke-test
./geo-bench --help
```

Go 1.19+ is required.

## Branch naming

Same as human contributors: `<type>/<short-description>` (e.g. `fix/off-by-one-in-pipeline`).

## Coding standards

- Match the style already in the file you are editing.
- Prefer clear, minimal changes over large refactors unless explicitly asked.
- Do not add comments that describe *what* the code does -- only add comments when the *why* is non-obvious.
- Do not introduce new dependencies without checking with the maintainer.
- Run `make checkfmt` (or `gofmt -d .`) before committing; the CI `make test` target enforces formatting.

## Running tests

```bash
make test
```

`make test` fetches dependencies, runs `gofmt` over every package, and then executes:

```bash
GO111MODULE=on go test -race -covermode=atomic ./...
```

A Redis instance on `localhost:6379` is required for the integration tests. Always run tests before declaring a task complete.

## How to submit changes

1. Create a branch: `git checkout -b <type>/<description>`.
2. Commit with a clear message focused on *why*, not *what*.
3. Open a pull request against `main`.
4. Do **not** push directly to `main`.

## What to avoid

- Do not reformat files unrelated to your change.
- Do not remove error handling or tests.
- Do not commit secrets, credentials, or large binary files.
- Do not amend published commits.