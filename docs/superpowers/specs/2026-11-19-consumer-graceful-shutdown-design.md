# Consumer graceful shutdown — short design

## Intent
Корректная остановка consumer по SIGTERM/SIGINT: без ложной ошибки, без потери
обработанного прогресса (офсеты коммитятся), без зависания на ребалансе.
Заодно закладываем шов для будущего DLQ.

## Goals
- Остановка по сигналу — штатная: `consumer.Run` возвращает `nil`, exit code 0.
- Семантика at-least-once: офсет коммитится только после обработки записи.
- При остановке: доработать текущую запись, закоммитить обработанное, закрыть клиент.
- Ошибка хендлера не ломает consumer: по умолчанию лог + пропуск.
- Решение «что делать с упавшей записью» вынесено в отдельный интерфейс (шов под DLQ).

## Non-goals
- DLQ, ретраи, классификация ошибок (retryable / non-retryable) — отдельная задача.
- Параллельная обработка, worker-пул.
- Изменения в producer.

## Anti-goals
- Пропуск записи из-за отмены контекста: если хендлер упал потому, что `ctx`
  отменён (shutdown), запись НЕ считается обработанной и НЕ коммитится.

## Approaches
- A. Починить цикл, коммит после батча, шов для failure policy — **выбран**.
- B. Автокоммит franz-go — отвергнут: коммитит polled-но-необработанное (at-most-once).
- C. Poll-loop + worker-пул с дренажом — отвергнут как избыточный сейчас.

## Design

### queue
- `Message` дополняется `Partition int32`, `Offset int64` (нужны логам и будущему DLQ).
- Хендлер: `type Handler func(ctx context.Context, msg Message) error`.
- Шов под DLQ:
  ```go
  type FailurePolicy interface {
      // OnFailure decides what to do with a record the handler failed on.
      // nil  -> record is considered handled (skipped/dead-lettered), offset may be committed.
      // err  -> consumer stops without committing this record.
      OnFailure(ctx context.Context, msg Message, cause error) error
  }
  ```
- Реализация сейчас: `LogAndSkip` (логирует с topic/partition/offset, возвращает nil).
  Позже: `DeadLetter` (publish в DLQ через `QueueProducer`, классификация ошибок).
- `queue.Consumer` держит handler + policy; `QueueConsumer.Consumer(ctx, handler, policy)`
  (точная форма — на усмотрение реализации, главное чтобы kafka-адаптер не знал про логирование).

### pkg/kafka (client.Consumer)
Цикл:
1. `PollFetches(ctx)`. Если `ctx.Err() != nil` — штатный выход (`nil`) раньше проверки `fetches.Errors()`.
   Прочие ошибки fetch — как сейчас, возвращаются.
2. По записям батча: `handler`; при ошибке — если `ctx.Err() != nil`, прервать без коммита этой записи;
   иначе `policy.OnFailure`; её ошибка → прервать.
   Контекст хендлера отвязан от отмены (`context.WithoutCancel`) и отменяется
   только через `handlerDrainTimeout` (5 с) после сигнала: текущая запись
   успевает доработаться, зависший хендлер не блокирует остановку. Запись,
   прерванная по таймауту, не коммитится и приходит повторно.
   Если `FailurePolicy` вернула ошибку при уже отменённом `ctx` — это тоже штатная
   остановка (`nil`), запись не коммитится.
3. После батча (или на прерывании — для уже обработанных): `CommitRecords` синхронно,
   с `context.WithoutCancel` + короткий таймаут (5 с), затем `AllowRebalance()`
   (`BlockRebalanceOnPoll` включён, без этого ребаланс/Close подвисают).
4. `cl.Close()` остаётся в closer (LIFO, 10 с на функцию).

### cmd/consumer
- После первого сигнала вызвать `stop()` (второй Ctrl+C убивает процесс сразу).
- Лог `shutdown complete` перенести после `application.Close`.

## Tests (fake-клиент, без реального Kafka)
- `ctx` отменён → `Run` возвращает `nil`.
- Ошибка хендлера + policy `nil` → запись пропущена, коммит включает её офсет.
- Ошибка хендлера при отменённом `ctx` → не коммитится.
- Ошибка policy → `Run` возвращает ошибку, запись не закоммичена.
Логика цикла, зависящая от `kgo.Client`, выносится за маленький интерфейс (poll/commit/allowRebalance), чтобы тестироваться без брокера — решение исполнителя.

## Левые решения (мои, можно поправить)
- `Partition`/`Offset` добавляем в существующий `queue.Message`, а не заводим отдельный тип.
- Таймаут финального коммита — 5 с (< 10 с closer).

## Решения после финального ревью
- Хендлер: drain-таймаут 5 с после сигнала (см. выше), а не мгновенная отмена.
- Ошибки fetch (в т.ч. `ErrDataLoss`) по-прежнему считаются фатальными. Классификация
  «фатально / игнорировать» — часть отдельной задачи про ретраи и DLQ.
- Если батч обрабатывается дольше rebalance-таймаута группы, коммит завершится ошибкой
  (`ILLEGAL_GENERATION`/`UNKNOWN_MEMBER`), consumer выйдет с ошибкой, записи придут повторно
  (допустимо для at-least-once).
