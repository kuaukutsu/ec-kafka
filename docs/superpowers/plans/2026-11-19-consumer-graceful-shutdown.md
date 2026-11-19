# Consumer Graceful Shutdown Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the consumer stop cleanly on SIGTERM/SIGINT (exit 0, offsets committed, no rebalance hang) and add a pluggable failure-policy seam for a future DLQ.

**Architecture:** The poll/handle/commit loop moves out of `kafka.client` into a testable `consume` function that depends on a tiny `poller` interface (satisfied by `*kgo.Client`). Per-record failures are delegated to `queue.FailurePolicy`; the default `LogAndSkip` logs and skips. Offsets are committed synchronously after each batch (at-least-once).

**Tech Stack:** Go 1.27.3, `github.com/twmb/franz-go v1.22.2` (`kgo`), `log/slog`, stdlib `testing` with hand-written fakes.

**Spec:** `docs/superpowers/specs/2026-11-19-consumer-graceful-shutdown-design.md`

## Global Constraints

- Module name is `kafka`; imports look like `kafka/internal/queue`. No new dependencies.
- All code and code comments in English.
- `context.Context` is always the first parameter.
- `os.Exit` only in `main()`; handlers/loops return `error`.
- Final commit uses `context.WithoutCancel(ctx)` + 5s timeout (`commitTimeout = 5 * time.Second`), shorter than the closer's 10s per-function budget.
- Before writing code load skills `go:coding-style`, `go:signature-consistency`, `go:unit-test` (tests are Given-When-Then, table-driven where it fits, hand-written fakes, `errors.Is/As` for error assertions).
- The directory is not a git repository: there are no commit steps; verification is by commands only.
- Verification baseline for every task: `go build ./... && go vet ./...`; for test tasks `go test -race -count=1 ./<pkg>`.

## Review Focus

- Ctx cancelled mid-batch with records still unprocessed → processed ones are committed, remaining are not handled, `consume` returns `nil`.
- Fetch error that is not a ctx cancel (e.g. `kgo.ErrClientClosed`) → `consume` returns a wrapped error, `AllowRebalance` still called.
- Commit failure (including the final commit on shutdown) → error is returned, not swallowed.
- Poll returns no records → no `CommitRecords` call, loop continues.
- Record with `nil` Key/Value → `queue.Message` has empty strings, no panic.
- Handler fails with `context.DeadlineExceeded` from its own timeout while `ctx` is alive → goes to the failure policy (not treated as shutdown).

(Each line is pinned by a test in Task 2.)

---

### Task 1: queue — Message fields, Handler, FailurePolicy, LogAndSkip

**Files:**
- Modify: `internal/queue/message.go` (add fields)
- Modify: `internal/queue/consumer.go` (new types, new signatures)
- Create: `internal/queue/failure_policy.go`
- Test: `internal/queue/failure_policy_test.go`, `internal/queue/consumer_test.go`

**Interfaces:**
- Produces:
  - `type Message struct { Topic, Key, Value string; Partition int32; Offset int64 }`
  - `type Handler func(ctx context.Context, msg Message) error`
  - `type FailurePolicy interface { OnFailure(ctx context.Context, msg Message, cause error) error }` — `nil` = record is handled (skipped/dead-lettered), non-nil = consumer must stop without committing this record.
  - `type QueueConsumer interface { Consumer(ctx context.Context, handler Handler, policy FailurePolicy) error }`
  - `func NewConsumer(client QueueConsumer, handler Handler, policy FailurePolicy) *Consumer`; `func (c *Consumer) Run(ctx context.Context) error` forwards to `client.Consumer(ctx, handler, policy)`.
  - `type LogAndSkip struct{ ... }`; `func NewLogAndSkip(log *slog.Logger) LogAndSkip`; `func (p LogAndSkip) OnFailure(ctx context.Context, msg Message, cause error) error`.

- [ ] **Step 1: Write failing tests**
  - `TestLogAndSkip_OnFailure`: Given a `slog` text handler writing to a `bytes.Buffer` and `Message{Topic:"t", Partition:3, Offset:42}`, When `OnFailure(ctx, msg, errBoom)`, Then it returns `nil` and the buffer contains `topic=t`, `partition=3`, `offset=42`, and `errBoom`'s text.
  - `TestConsumer_Run_ForwardsToClient`: Given a fake `QueueConsumer` that records the handler/policy it received and returns `errBoom`, When `NewConsumer(fake, h, p).Run(ctx)`, Then the fake got the same `ctx`, `h` was invoked through it (call it, assert it ran), `p` equals the given policy, and `Run` returns an error satisfying `errors.Is(err, errBoom)`.

- [ ] **Step 2: Run** `go test ./internal/queue` — Expected: FAIL (compile errors: undefined `LogAndSkip`, `Handler`, ...).

- [ ] **Step 3: Implement** the types/signatures above. `LogAndSkip.OnFailure` logs at `Error` level with attributes `topic`, `partition`, `offset`, `err`, then returns `nil`.

- [ ] **Step 4: Run** `go test -race -count=1 ./internal/queue` — Expected: PASS. (`go build ./...` will fail in `pkg/kafka` and `internal/app` until Tasks 2–3; that is expected here.)

---

### Task 2: pkg/kafka — graceful consume loop

**Files:**
- Create: `pkg/kafka/consume.go`
- Modify: `pkg/kafka/client.go` (replace the `Consumer` method body)
- Test: `pkg/kafka/consume_test.go`

**Interfaces:**
- Consumes: `queue.Handler`, `queue.FailurePolicy`, `queue.Message`, `queue.QueueConsumer` (Task 1).
- Produces:
  - `type poller interface { PollFetches(ctx context.Context) kgo.Fetches; CommitRecords(ctx context.Context, rs ...*kgo.Record) error; AllowRebalance() }` (satisfied by `*kgo.Client`).
  - `const commitTimeout = 5 * time.Second`
  - `func consume(ctx context.Context, p poller, handle queue.Handler, policy queue.FailurePolicy) error`
  - `func (c *client) Consumer(ctx context.Context, handler queue.Handler, policy queue.FailurePolicy) error` → `return consume(ctx, c.cl, handler, policy)`.

**Behavior of `consume`** (one iteration = one poll):
1. `fetches := p.PollFetches(ctx)`. If `ctx.Err() != nil` → return `nil` (check this **before** `fetches.Errors()`).
2. If `fetches.Errors()` non-empty → return `fmt.Errorf("fetch: %w", errors.Join(<each FetchError.Err>...))`.
3. Iterate with `fetches.RecordIter()` in order. Convert `*kgo.Record` → `queue.Message` (`string(Key)`, `string(Value)`, `Partition`, `Offset`). On handler error:
   - `ctx.Err() != nil` → stop the batch, the failed record is **not** added to `processed`, final return `nil`;
   - otherwise `policy.OnFailure(ctx, msg, err)`; non-nil result → stop the batch, record not processed, return `fmt.Errorf("failure policy: %w", err)`;
   - nil result → record counts as processed.
4. After the batch (also when stopped early) if `processed` is non-empty: `CommitRecords(commitCtx, processed...)` with `commitCtx` = `context.WithTimeout(context.WithoutCancel(ctx), commitTimeout)`; a commit error is returned as `fmt.Errorf("commit: %w", err)` (takes precedence over a nil result, joined with a policy error if both).
5. `p.AllowRebalance()` is called exactly once per poll, after the commit (use a per-iteration helper with `defer`).

- [ ] **Step 1: Write failing tests** with a hand-written `fakePoller` (scripted `[]kgo.Fetches`; when the script is exhausted it blocks until `ctx.Done()` then returns a `Fetches` containing a `context.Canceled` `FetchError`; records `CommitRecords` calls and an `allowRebalance` counter). Build `kgo.Fetches` literals from the public structs (`Fetch{Topics: []FetchTopic{{Topic, Partitions: []FetchPartition{{Partition, Records}}}}}`). Tests (GWT, table-driven where it fits):
  - `TestConsume_CtxCancelled_ReturnsNil`: cancel ctx while the fake blocks → `nil`, no commit, `allowRebalance >= 1`.
  - `TestConsume_BatchProcessed_CommitsOnceAfterBatch`: 3 records, handler ok → one `CommitRecords` call with those 3 records, then `AllowRebalance` after it (assert call order via a shared event log).
  - `TestConsume_HandlerError_PolicyNil_RecordCommitted`: record 2 fails, policy returns nil and was called with the right `Message` (`Partition`, `Offset`) and cause → all 3 records committed.
  - `TestConsume_HandlerError_CtxCancelled_NotCommitted`: handler cancels ctx and returns `context.Canceled` on record 2 → only record 1 committed, record 3 never handled, policy never called, returns `nil`.
  - `TestConsume_PolicyError_StopsWithoutCommittingRecord`: policy returns `errBoom` on record 2 → record 1 committed, record 2 not, returns error with `errors.Is(err, errBoom)`.
  - `TestConsume_FetchError_ReturnsWrapped`: fetch with `FetchError{Err: kgo.ErrClientClosed}` (live ctx) → `errors.Is(err, kgo.ErrClientClosed)`, `allowRebalance == 1`.
  - `TestConsume_CommitError_Returned`: fake `CommitRecords` returns `errBoom` → `errors.Is(err, errBoom)`.
  - `TestConsume_EmptyPoll_NoCommit`: a fetch with no records, then ctx cancel → `CommitRecords` never called.
  - `TestConsume_NilKeyValue_EmptyStrings`: record with `Key: nil, Value: nil` → handler receives `Message{Key:"", Value:""}`.
  - `TestConsume_HandlerDeadlineExceeded_LiveCtx_GoesToPolicy`: handler returns `context.DeadlineExceeded`, ctx alive → policy called, record committed.
  - `TestConsume_FinalCommitUsesUncancelledCtx`: handler cancels ctx after succeeding on record 1 and the batch continues to the end condition; the fake asserts the ctx passed to `CommitRecords` has `ctx.Err() == nil` and a deadline within `commitTimeout`.

- [ ] **Step 2: Run** `go test ./pkg/kafka` — Expected: FAIL (undefined `consume`, `poller`).

- [ ] **Step 3: Implement** `consume.go` per the behavior above and update `client.Consumer` in `client.go` (delete the old loop and the `fn func(string)` signature). Keep functions small (cyclop limit 20 in `.golangci.yml`): split into `consume` (poll loop), `processBatch`, `toMessage`, `commit`.

- [ ] **Step 4: Run** `go test -race -count=2 -cpu=1,2 ./pkg/kafka` — Expected: PASS.

---

### Task 3: Wiring and `main`

**Files:**
- Modify: `internal/app/queue.go` (`MakeConsumer`)
- Modify: `cmd/consumer/main.go`

**Interfaces:**
- Consumes: `queue.NewConsumer(client, handler, policy)`, `queue.NewLogAndSkip(log)` (Task 1); `kafka` client satisfies `queue.QueueConsumer` (Task 2).

- [ ] **Step 1: Update `MakeConsumer`** to pass a `queue.Handler` (`func(ctx context.Context, msg queue.Message) error { fmt.Println(msg.Value); return nil }` — same behavior as today) and `queue.NewLogAndSkip(a.log)`.

- [ ] **Step 2: Update `cmd/consumer/main.go`:**
  - in `run`, right after creating the signal context start `go func() { <-ctx.Done(); stop() }()` so a second SIGINT/SIGTERM kills the process with default behavior;
  - remove the `consumer: shutdown complete` log from `run`; in `main`, after `run` returns `nil` (all deferred `application.Close` have completed), log `consumer: shutdown complete`.

- [ ] **Step 3: Verify** — `go build ./... && go vet ./... && make lint-check && go test -race -count=2 -cpu=1,2 ./...`. Expected: all pass, no new lint findings.

- [ ] **Step 4: Manual check (needs brokers on `localhost:19092,29092,39092`; skip and report if unavailable):** `make run consumer`, send `SIGTERM` (`kill -TERM <pid>`). Expected: log shows no `consumer: fatal`, the process exits with code 0 within a few seconds, the log line `consumer: shutdown complete` is the last one; restarting does not replay already handled records (committed offsets for group `gokafka`).

---

## Self-Review notes
- Spec coverage: штатный выход (T2 step 1/ctx test), at-least-once коммит после батча (T2), финальный коммит с WithoutCancel (T2), failure policy + LogAndSkip (T1), anti-goal «ctx-ошибка хендлера не пропускается» (T2), `Partition`/`Offset` (T1), второй сигнал и порядок логов (T3), тесты на fake (T2).
- Signatures are identical across tasks (`Handler`, `FailurePolicy`, `consume`, `poller`).
