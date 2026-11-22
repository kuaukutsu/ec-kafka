# Repository Guidelines

## Project Structure & Module Organization

This Go project demonstrates Kafka producer and consumer workflows using franz-go. Entry points live in `cmd/producer` and `cmd/consumer`. Application lifecycle and logging are in `internal/app`, configuration in `internal/config`, and queue behavior in `internal/queue`. Reusable Kafka client code is in `pkg/kafka`. Tests sit beside their implementation in `*_test.go` files. Local settings are in `config/local.yaml`; design and review documents are in `docs/`.

## Build, Test, and Development Commands

- `go build ./...` builds all packages.
- `go test ./...` runs tests across the repository, including `pkg/kafka`.
- `make tests` clears the test cache and runs tests under `internal/`; its recipe ignores failures, so use `go test` when checking the exit status.
- `make run producer` or `make run consumer` starts an entry point with the local configuration. A separately provisioned Kafka cluster is required; see the README for the expected setup.
- `make fmt` formats Go code with gofumpt; `make fmt-check` lists files needing formatting.
- `make lint-check` runs golangci-lint without changes; `make lint` applies supported fixes. These targets install their pinned tools into `.bin` if missing.

## Coding Style & Naming Conventions

Follow standard Go conventions, including tab indentation and concise package names. Run `make fmt` before submitting changes and follow `.golangci.yml`. Name Go source files in lowercase with underscores where helpful, and keep tests alongside the code they exercise.

## Testing Guidelines

Use Go's standard `testing` package and name test files `*_test.go` and test functions `TestXxx`. Add focused tests for behavior changes, including failure and shutdown paths where applicable. Run `go test ./...` before submitting; no coverage threshold is defined in the repository.

## Commit & Pull Request Guidelines

Recent commits use short, imperative descriptions such as `fix readme` and sometimes a scope prefix such as `example:`. Keep commits focused and describe the change clearly. For pull requests, explain the behavior change, list verification commands and results, and link any relevant issue. Include configuration or Kafka setup details when needed to reproduce the change.

## Communication Guidelines

- write all code and code comments in English
- communicate with the user in Russian