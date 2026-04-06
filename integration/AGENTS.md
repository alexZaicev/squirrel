# AGENTS.md — integration tests

## Overview

This directory contains integration tests that verify Squirrel against real databases. It is a **separate Go module** (`github.com/Masterminds/squirrel/integration`) that depends on the parent module via a `replace` directive in `go.mod`.

## Prerequisites

These tests require live database instances. They are **not** part of the main test suite and do not run with `go test ./...` from the project root.

Supported databases:
- **MySQL** — driver: `github.com/go-sql-driver/mysql`
- **PostgreSQL** — driver: `github.com/lib/pq`
- **SQLite** — driver: `github.com/mattn/go-sqlite3` (requires CGO)

## Running integration tests

```sh
cd integration
go test -v ./...
```

For SQLite tests, ensure CGO is enabled: `CGO_ENABLED=1 go test -v ./...`

## Key details

- The module uses its own `go.mod` and `go.sum` — dependency changes here do not affect the parent module.
- The `replace github.com/Masterminds/squirrel => ../` directive points to the local parent, so tests always run against the current source.
- Test assertions use `github.com/stretchr/testify v1.4.0` (a newer version than the parent module).
- Do not modify this module's `go.mod` unless you are adding or updating database driver dependencies.
- If you modify the parent module's public API, verify integration tests still compile by running `go build ./...` from this directory.
