# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Communication Guidelines

- write all code and code comments in English
- communicate with the user in Russian

## Project

A Go playground for Kafka (client library: `github.com/twmb/franz-go`). Go module name is `kafka` (imports look like `kafka/internal/app`). Toolchain versions are pinned in `.tool-versions` and the `Makefile` (`make goset` sets Go via asdf). The code is early-stage and partly stubbed (see "Current state").

## Commands

The `Makefile` takes positional arguments via `ARGS` (a catch-all `%` rule swallows them), so write `make run consumer`, not `make run ARGS=consumer`.

- `make run <consumer|producer>` — runs `cmd/<name>/main.go` with `-tags local --config=./config/local.yaml`
- `make test [<pkg>]` — `go test ./<pkg>` and then `go test -race -count=2 -cpu=1,2` (both steps are prefixed with `-`, so failures don't stop make). Run a single test directly: `go test ./internal/app -run TestName`
- `make lint` — golangci-lint with `--fix`; `make lint-check` — without fix
- `make fmt` — `gofumpt -extra -l -w .`; `make fmt-check` — list only
- Lint/format tools are installed into `./.bin` on first use (`.bin` is gitignored). The linter config is `.golangci.yml` (v2 format; notable: cyclop max complexity 20, gosec, errorlint, depguard bans `io/ioutil`; its comments are in Russian).
- Kafka brokers come from `queue.seed` in the config (comma-separated; `config.Queue.Brokers()` trims spaces). `config/local.yaml` expects `localhost:19092`, `29092`, `39092`. There is no docker-compose for them in this repo.
- `make claude` / `make herdr` (from `.claude/Makefile`) build and enter the Docker dev container for Claude Code; not related to the app itself.

## Architecture

Two binaries under `cmd/` (`consumer`, `producer`), each with its own `main()`; shared code lives in `internal/` and `pkg/`.

The consumer is a long-running process. The producer is not: `queue.Producer` is a command that application code calls at runtime whenever data needs to be sent (the topic is part of each `queue.Message`, not of the config). `cmd/producer` is only a demo harness that sends one message. The process that embeds a producer owns its lifecycle: build it with `app.MakeProducer` and close it through the `closer`.

- **Config** (`internal/config`): `config.LoadFromArgs(os.Args[1:])` resolves the path (`--config` flag via a private flag set, falling back to the `CONFIG_PATH` env var) and calls `config.Load(path)`, which reads a YAML file via `cleanenv` and validates `Env`. Both return errors instead of panicking; the `main` functions print the error and exit 1. `Config.Env` (`local` / `stage` / `production`) selects logging behavior. Values can also come from ENV, and ENV has priority over the file (`KAFKA_SEED`; `KAFKA_CONSUMER_GROUP` and `KAFKA_CONSUMER_TOPIC` for `queue.consumer.group` / `queue.consumer.topic`). `MakeProducer` and `MakeConsumer` require a non-empty seed (`Queue.Validate`), `MakeConsumer` also requires group and topic.
- **Logging** (`internal/app/logger.go`): `slog`; text at Debug for `local`, JSON at Warn for `production`, JSON at Info otherwise.
- **App wiring** (`internal/app`): `app.NewApp(log)` builds an `App` holding the logger and a `Closer`; `app.SignalContext` is the shared SIGINT/SIGTERM context for both `main`s. The `Closer` (`closer.go`) is a LIFO shutdown registry. `Add(name, fn)` registers cleanup functions, and `Close` runs each in its own goroutine with a 10s per-function timeout, ignores cancellation of the parent context (`context.WithoutCancel`, so it still works after SIGTERM), joins the errors, and is idempotent through `sync.Once`. A function that outlives its timeout is abandoned (its late result is only logged), so a hung function cannot block `Close`. `app.MakeConsumer` / `MakeProducer` in `queue.go` build Kafka clients from the config and register them in the closer.
- **Queue abstraction** (`internal/queue`): transport-agnostic `Message` (with `Partition`/`Offset` on the consuming side), `Producer` (depends on `ProducerClient.ProduceSync`) and `Consumer` (depends on `ConsumerClient.Consume`). Consuming is `Handler func(ctx, Message) error` plus a `FailurePolicy` that decides what to do with a record the handler failed on: `nil` means handled (skipped / dead-lettered), an error stops the consumer without committing that record. The only policy today is `LogAndSkip`; this is the seam for a future DLQ.
- **Kafka adapter** (`pkg/kafka`): wraps `kgo.Client`. `New` (dial and ping; `Timeout` sets both timeouts, ping defaults to 1s) returns a `Client` that implements both `queue.ProducerClient` and `queue.ConsumerClient`. `options.go` has functional options (`SeedBrokers`, `Timeout`, `RecordDeliveryTimeout`, `ProducerDefault`, `ConsumerDefault`). The consume loop lives in `consume.go` behind a small `poller` interface (satisfied by `*kgo.Client`, faked in tests). Note the dependency direction: `pkg/kafka` imports `internal/queue`.

## Graceful shutdown of the consumer

- `cmd/consumer` cancels the root context on SIGINT/SIGTERM and calls `stop()` right after, so a second signal kills the process.
- Stopping on a signal is normal: `consume` returns `nil` (exit code 0). The context check goes before fetch errors, because a cancelled poll reports `context.Canceled` as a fetch error.
- Semantics are at-least-once. Offsets are committed synchronously after every batch (`DisableAutoCommit` + `BlockRebalanceOnPoll` are on, so `AllowRebalance` is called after every poll). The final commit uses a context detached from cancellation with a 5s timeout.
- The handler gets a context that is cancelled only `handlerDrainTimeout` (5s) after the signal, so the record in flight can finish. A record whose handler or failure policy failed because of the shutdown is never committed or skipped; it is redelivered.
- The client is closed by the `closer` after `consume` has returned. `consumer: shutdown complete` is logged after that.

## Current state (things that look finished but are not)

- Fetch errors (including `ErrDataLoss`) are all fatal for the consumer. Deciding which are retryable/ignorable belongs to the future retry/DLQ task.
- There is no DLQ or retry yet, only `LogAndSkip`. A failure policy that keeps returning errors makes the consumer crash-loop on a poisoned record.
- The business handler (it just prints the value) is built inside `app.MakeConsumer`; it should eventually be passed in from outside.
- Real-broker behavior of the shutdown (rebalance, `Close`, commit after a long batch) has only been covered by unit tests with a fake poller, not by an integration test.
- `make lint-check` reports `gofumpt` vs `gci` import-grouping conflicts across the repo (the module name `kafka` has no dot, so `gofumpt` treats it as standard library).
