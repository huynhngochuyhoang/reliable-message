# Milestone 14 Rabbit WebFlux Bridge Checklist

This milestone adds RabbitMQ support for WebFlux systems as a blocking bridge, hybrid mode, and migration support. It must not be described as native Reactor RabbitMQ support.

## Current Status

- [x] Phase 14.1 through Phase 14.8 define the current implemented event bridge direction.
- [x] Event publishing uses `RabbitTemplate` through `ReactiveRabbitBridgePublisher`.
- [x] Event consuming uses Strategy A with ack after handler `Mono` and idempotency success complete.
- [x] Bridge execution is isolated behind explicit platform or virtual-thread executor modes.
- [x] Overload behavior is bounded with fail-fast rejection.
- [x] Bridge metrics use `runtime=webflux-bridge`, `transport=rabbit`, and `executor_mode` tags.
- [x] Phase 14.8.1 adds reactive R2DBC outbox flushing for event publishers.
- [x] Phase 14.8.2 adds dialect-aware outbox schema resolution and PostgreSQL JSON binding.
- [x] Phase 14.8.3 adds generic and PostgreSQL optimized outbox claim strategies.
- [x] Phase 14.9 through Phase 14.11 keep Rabbit RPC in a separate `AsyncRabbitTemplate`-based module.
- [x] Phase 14.10.1 adds RPC response typing, envelope handling, and bounded virtual-thread mode.
- [x] Phase 14.12 completes the user-facing README and usage documentation pass.

## Implemented Direction

- [x] Keep the event bridge honest as blocking bridge, hybrid mode, and migration support.
- [x] Treat virtual threads as optimized blocking support, not as reactive RabbitMQ.
- [x] Keep `ReactiveReliablePublisher` separate from `ReactiveRabbitRpcClient`.
- [x] Keep event retry/DLQ separate from RPC timeout, retry, and bulkhead semantics.

## Global Guardrails

- [x] Keep event messaging and RPC in separate modules and APIs.
- [x] Use `RabbitTemplate` for event messaging only.
- [x] Use `AsyncRabbitTemplate` for RPC only.
- [x] Do not use outbox for RPC by default.
- [x] Keep optional R2DBC outbox flushing event-messaging only.
- [x] Do not hide blocking RabbitMQ calls behind `Mono` without an explicit bridge executor.
- [x] Do not run blocking RabbitMQ work on Netty event-loop threads.
- [x] Use bounded executors, bounded queues, and bounded concurrency.
- [x] Implement only `fail-fast` rejection in the first version.
- [x] Do not implement `block-caller` rejection yet.
- [x] Do not implement `drop-and-metric` rejection yet.
- [x] Ack only after the handler `Mono` completes successfully.
- [x] Do not implement Strategy B async ack coordination yet.
- [x] Treat virtual threads only as a blocking bridge executor option.
- [x] Do not claim fully reactive, fully non-blocking, exactly-once, or unlimited concurrency semantics.

## Phase 14.1 Event Bridge Module Scaffold And Properties

Goal:
- [x] Create the event messaging bridge module with configuration only.

Implementation tasks:
- [x] Add `reliable-message-rabbit-webflux-bridge/pom.xml`.
- [x] Add the module to the root `pom.xml`.
- [x] Add `RabbitWebFluxBridgeProperties`.
- [x] Add `RabbitWebFluxBridgeAutoConfiguration`.
- [x] Add Spring Boot auto-configuration imports.
- [x] Use wording and package names that make this a blocking bridge, hybrid mode, and migration support module.

Tests to write first:
- [x] Auto-configuration backs off when disabled.
- [x] Properties bind `executor-mode`, `worker-threads`, `queue-capacity`, `max-concurrency`, and `rejection-policy`.
- [x] Invalid `max-concurrency <= 0` fails clearly.
- [x] Invalid negative `queue-capacity` fails clearly.
- [x] Invalid platform worker count fails clearly.

Success criteria:
- [x] Module compiles.
- [x] No Rabbit publish behavior exists yet.
- [x] No Rabbit consume behavior exists yet.
- [x] Defaults cannot create unbounded executor behavior.

Risks:
- [x] Module naming accidentally suggests full reactive RabbitMQ support.
- [x] Defaults accidentally permit unbounded queueing later.

Out of scope:
- [x] Publisher implementation.
- [x] Listener implementation.
- [x] RPC implementation.
- [x] Outbox integration.
- [x] Metrics.

## Phase 14.2 Bridge Executor And Scheduler

Goal:
- [x] Provide explicit blocking isolation for Rabbit event bridge work.

Implementation tasks:
- [x] Add `RabbitBridgeExecutorProvider`.
- [x] Add `PlatformThreadRabbitBridgeExecutorProvider`.
- [x] Add `VirtualThreadRabbitBridgeExecutorProvider`.
- [x] Add a bridge scheduler or executor adapter backed by the configured executor.
- [x] Name bridge threads clearly.
- [x] Keep platform executor queue bounded.

Tests to write first:
- [x] Platform executor uses bounded worker count.
- [x] Platform executor uses bounded queue capacity.
- [x] Platform executor rejects when saturated.
- [x] Virtual-thread mode still depends on the external concurrency guard.
- [x] Bridge work does not use Reactor `parallel`, Netty event loop, or `ForkJoinPool.commonPool` by default.

Success criteria:
- [x] Blocking bridge executor is explicit and injectable.
- [x] No unbounded platform queue exists.
- [x] Virtual threads are available only as a blocking bridge executor option.

Risks:
- [x] Virtual-thread executor accepts too many submitted tasks without a guard.
- [x] Rejection behavior is hard to test if executor details leak.

Out of scope:
- [x] RabbitTemplate publish calls.
- [x] Listener ack behavior.
- [x] RPC executor behavior.

## Phase 14.3 Concurrency Guard And Fail-Fast Rejection

Goal:
- [x] Add bounded concurrency before any blocking Rabbit bridge work is submitted.

Implementation tasks:
- [x] Add `RabbitBridgeConcurrencyGuard`.
- [x] Add `RabbitBridgeRejectedException`.
- [x] Support `fail-fast` rejection only.
- [x] Acquire permits before executor submission.
- [x] Release permits on success, failure, cancellation, and executor rejection.

Tests to write first:
- [x] Permit is released on success.
- [x] Permit is released on failure.
- [x] Permit is released on cancellation.
- [x] Saturation fails fast.
- [x] Executor rejection does not leak a permit.
- [x] No unbounded queueing is introduced.

Success criteria:
- [x] Every bridge operation must pass through the guard before execution.
- [x] Saturation produces a controlled error.
- [x] No `block-caller` or `drop-and-metric` behavior exists.

Risks:
- [x] Permit is released without being acquired.
- [x] Permit leaks on cancellation.
- [x] Work is queued before a permit is acquired.

Out of scope:
- [x] `block-caller` rejection.
- [x] `drop-and-metric` rejection.
- [x] Metrics.
- [x] RabbitTemplate integration.

## Phase 14.4 Reactive Event Publisher Bridge

Goal:
- [x] Implement WebFlux-facing event publish through `RabbitTemplate`, isolated by the bridge executor and concurrency guard.

Implementation tasks:
- [x] Add `ReactiveRabbitBridgePublisher`.
- [x] Wire it as a `ReactiveReliablePublisher` implementation.
- [x] Serialize messages using the existing reliable message format.
- [x] Publish events with `RabbitTemplate`.
- [x] Offload blocking publish work through the bridge executor.
- [x] Guard publish calls with the concurrency guard.
- [x] Ensure `AsyncRabbitTemplate` is not used.

Tests to write first:
- [x] `publish(...)` calls `RabbitTemplate.convertAndSend`.
- [x] Publish runs on the bridge executor, not the caller thread.
- [x] Permit is released after successful publish.
- [x] Permit is released after publish failure.
- [x] Executor or guard saturation returns a `Mono` error.
- [x] Serialization failure does not call `RabbitTemplate`.
- [x] No `AsyncRabbitTemplate` dependency or usage exists in the event bridge module.

Success criteria:
- [x] Event publish works through `RabbitTemplate` only.
- [x] Blocking publish does not run inline on the subscriber thread.
- [x] No outbox integration exists in this phase.

Risks:
- [x] Blocking work is wrapped with `Mono.just(...)`.
- [x] `subscribeOn` uses an uncontrolled scheduler.
- [x] Event publishing accidentally gains RPC semantics.

Out of scope:
- [x] Listener implementation.
- [x] Retry and DLQ integration.
- [x] RPC.
- [x] Outbox persistence.

## Phase 14.5 Reactive Event Listener Bridge With Strategy A

Goal:
- [x] Consume Rabbit messages with Spring AMQP listener infrastructure and invoke reactive handlers with simple ack-after-completion semantics.

Implementation tasks:
- [x] Add `ReactiveRabbitBridgeListenerEndpoint`.
- [x] Add `ReactiveRabbitBridgeListenerMethodInvoker`.
- [x] Add `ReactiveRabbitBridgeListenerRegistrar`.
- [x] Add `ReactiveRabbitBridgeMessageHandler`.
- [x] Wire Spring AMQP listener container support.
- [x] Invoke handler `Mono` and wait for completion using Strategy A.
- [x] Ack only after handler `Mono` completes successfully.

Tests to write first:
- [x] `@ReactiveReliableListener` method returning `Mono<Void>` is invoked.
- [x] Non-public listener method can be invoked.
- [x] Handler `Mono` is awaited before ack.
- [x] Ack happens only after delayed `Mono` completion.
- [x] Handler error does not ack as success.
- [x] No Strategy B async ack coordination is present.

Success criteria:
- [x] Strategy A listener bridge works.
- [x] Ack-after-success is proven by tests.
- [x] Blocking wait is explicit and isolated to the listener bridge boundary.

Risks:
- [x] Ack happens before `Mono` completion.
- [x] Listener blocks a thread that should not be blocked.
- [x] Reactor Context is lost.

Out of scope:
- [x] Strategy B async ack coordination.
- [x] Retry and DLQ routing.
- [x] RPC.
- [x] Metrics.

## Phase 14.6 Listener Failure, Duplicate, And Idempotency Semantics

Goal:
- [x] Add event messaging reliability semantics around reactive Rabbit consume flow.

Implementation tasks:
- [x] Integrate `ReactiveIdempotencyStore`.
- [x] Handle duplicate `SUCCESS` without invoking the handler.
- [x] Handle duplicate `PROCESSING` and `FAILED` without acking as success.
- [x] Mark success only after handler `Mono` completes.
- [x] Mark failure when handler or idempotency flow fails.
- [x] Expose a Rabbit event failure hook and record retry/DLQ outcomes when the hook returns a concrete outcome.

Tests to write first:
- [x] New message runs `tryStart`, handler `Mono`, `markSuccess`, then ack.
- [x] Duplicate `SUCCESS` acks without invoking handler.
- [x] Duplicate `PROCESSING` does not ack as success.
- [x] Duplicate `FAILED` does not ack as success.
- [x] Handler failure marks failure and does not ack as success.
- [x] Idempotency store failure does not silently ack.
- [x] Failure-hook outcomes remain event-messaging-only and do not add Reactor retry or advanced topology creation.

Success criteria:
- [x] Duplicate behavior cannot silently lose messages.
- [x] Failure behavior is explicit.
- [x] Retry and DLQ stay event messaging concerns.

Risks:
- [x] Reactor retry is mistaken for Rabbit business retry.
- [x] Idempotency failure is treated as business success.
- [x] Ack/nack behavior conflicts with broker retry configuration.

Out of scope:
- [x] RPC.
- [x] Outbox integration.
- [x] Strategy B async ack coordination.
- [x] Advanced observability.

## Phase 14.7 Event Loop Protection And Overload Behavior

Goal:
- [x] Make unsafe bridge usage visible and keep overload behavior bounded.

Implementation tasks:
- [x] Add event-loop detection helper.
- [x] Warn or record a safety signal when publish is called from a Reactor HTTP event-loop thread.
- [x] Ensure publish is still offloaded to the bridge executor.
- [x] Ensure saturation fails fast.
- [x] Ensure cancellation releases concurrency resources.

Tests to write first:
- [x] Publish called from a Reactor HTTP-style thread name emits a warning or safety signal.
- [x] Publish called from an event-loop-style thread still offloads blocking work.
- [x] Saturated concurrency guard fails fast.
- [x] Cancellation releases resources.
- [x] No unbounded queue path exists.

Success criteria:
- [x] Blocking calls remain isolated.
- [x] Overload behavior is deterministic.
- [x] Safety checks do not change successful business behavior.

Risks:
- [x] Thread-name detection is imperfect.
- [x] Safety checks become noisy.
- [x] Protection logic becomes more complex than the bridge itself.

Out of scope:
- [x] BlockHound integration.
- [x] Load testing.
- [x] Non-fail-fast rejection policies.

## Phase 14.8 Event Observability

Goal:
- [x] Add bridge metrics and optional tracing signals for event publish and consume.

Implementation tasks:
- [x] Add `RabbitBridgeMetrics`.
- [x] Record publish success and failure.
- [x] Record consume success, failure, and duplicate outcomes.
- [x] Record retry and DLQ outcomes when event failure hooks expose concrete outcomes.
- [x] Record executor active, queued, and rejected counts where available.
- [x] Tag metrics with `runtime=webflux-bridge`.
- [x] Tag metrics with `transport=rabbit`.
- [x] Tag metrics with `executor_mode=platform|virtual-thread`.

Tests to write first:
- [x] Publish success counter increments.
- [x] Publish failure counter increments.
- [x] Consume success counter increments.
- [x] Consume failure counter increments.
- [x] Duplicate counter increments.
- [x] Executor rejection counter increments.
- [x] Metrics include bridge runtime and Rabbit transport tags.

Success criteria:
- [x] Operators can see bridge saturation and failures.
- [x] Metrics do not change business flow.
- [x] Metrics wording reflects blocking bridge and hybrid mode.

Risks:
- [x] Metrics duplicate existing observability abstractions.
- [x] Queue size gauges differ between platform and virtual-thread modes.

Out of scope:
- [x] RPC metrics.
- [x] Mandatory distributed tracing dependency.

## Phase 14.8.1 Reactive R2DBC Outbox Flusher Wiring

Goal:
- [x] Flush durable reactive event rows through the active `ReactiveReliablePublisher`.

Implemented direction:
- [x] Add `ReactiveOutboxFlushScheduler` and `R2dbcOutboxProperties`.
- [x] Create the scheduler only when outbox and flushing are enabled and both store and publisher beans exist.
- [x] Read claimed rows, publish through `ReactiveReliablePublisher`, mark published only after success, and mark failed with retry metadata on error.
- [x] Skip overlapping flush ticks and keep per-batch work bounded.
- [x] Keep RPC outbox disabled by default.

## Phase 14.8.2 R2DBC Outbox Schema Configuration

Goal:
- [x] Keep R2DBC outbox DDL portable without drifting from runtime binding behavior.

Implemented direction:
- [x] Resolve column types from explicit config, dialect recommendation, then generic fallback.
- [x] Support `text` and `json` payload storage modes.
- [x] Use dialect-aware PostgreSQL `json/jsonb` binding.
- [x] Fail fast for planned `binary` storage until runtime codec and `payload_bytes` read/write support exist.
- [x] Provide PostgreSQL, MySQL, Oracle, SQL Server, and generic defaults.

## Phase 14.8.3 R2DBC Outbox Claim Strategy

Goal:
- [x] Isolate dialect-aware outbox claiming while preserving portable fallback behavior.

Implemented direction:
- [x] Add `OutboxClaimStrategy`.
- [x] Keep generic select-ID plus conditional-update claiming as fallback.
- [x] Add PostgreSQL atomic `FOR UPDATE SKIP LOCKED` plus `UPDATE ... RETURNING` claiming.
- [x] Keep retry eligibility and processing lease behavior unchanged.
- [x] Avoid window functions in the PostgreSQL locked query.

## Phase 14.9 Rabbit RPC WebFlux Bridge Module Scaffold

Goal:
- [x] Create a separate RPC bridge module using `AsyncRabbitTemplate`.

Implementation tasks:
- [x] Add `reliable-message-rpc-rabbit-webflux-bridge/pom.xml`.
- [x] Add the module to the root `pom.xml`.
- [x] Add `ReactiveRabbitRpcClient`.
- [x] Add `RabbitRpcWebFluxBridgeProperties`.
- [x] Add `RabbitRpcWebFluxBridgeAutoConfiguration`.
- [x] Keep package and class names clearly separate from event messaging.

Tests to write first:
- [x] Auto-configuration creates RPC client only when `AsyncRabbitTemplate` exists.
- [x] RPC auto-configuration does not create event publisher beans.
- [x] RPC auto-configuration does not create event listener beans.
- [x] RPC module does not depend on outbox by default.
- [x] RPC module does not use `RabbitTemplate`.

Success criteria:
- [x] RPC bridge is physically separate from the event bridge module.
- [x] `AsyncRabbitTemplate` appears only in the RPC bridge.
- [x] No outbox behavior exists in RPC defaults.

Risks:
- [x] Shared configuration blurs event and RPC semantics.
- [x] RPC code accidentally depends on event bridge classes.

Out of scope:
- [x] Event messaging.
- [x] Outbox.
- [x] `RabbitTemplate`.
- [x] Retry, circuit breaker, and bulkhead behavior.

## Phase 14.10 Rabbit RPC Request/Response Client

Goal:
- [x] Implement Rabbit request/reply over `AsyncRabbitTemplate` with a WebFlux-friendly `Mono` boundary.

Implemented direction:
- [x] Add `DefaultReactiveRabbitRpcClient`.
- [x] Offload `AsyncRabbitTemplate` invocation to the dedicated RPC bridge executor.
- [x] Apply caller-visible timeout without blocking caller/event-loop threads.
- [x] Propagate logical RPC headers and generate a fresh physical AMQP correlation ID per retry attempt.
- [x] Surface transport/future and conversion failures.
- [x] Keep cancellation honest: client-side cancellation may not cancel broker-side or remote work.

## Phase 14.10.1 Rabbit RPC Client Hardening

Goal:
- [x] Add typed responses, application error envelopes, and bounded executor modes.

Implemented direction:
- [x] Support `ParameterizedTypeReference<T>`.
- [x] Keep raw response mode backward-compatible.
- [x] Add explicit `RpcResponseEnvelope<T>` handling and map `ERROR` to `RabbitRpcRemoteException`.
- [x] Support platform and named virtual-thread RPC executor modes.
- [x] Keep both modes bounded by `max-concurrency` and fail fast on saturation.
- [x] Hold permits for active bridge work until the returned future reaches a terminal state.

## Phase 14.11 Rabbit RPC Retry, Bulkhead, And Metrics

Goal:
- [x] Add normal RPC resilience without event outbox, retry queue, or DLQ semantics.

Implemented direction:
- [x] Retry bounded retryable timeout and transient transport/future failures.
- [x] Do not retry remote `ERROR` envelopes or conversion failures by default.
- [x] Reuse RPC executor `max-concurrency` as the fail-fast bounded bulkhead.
- [x] Record RPC-specific request, success, failure, timeout, retry, bulkhead rejection, and duration metrics.
- [x] Keep RPC metrics separate from event bridge metrics.
- [ ] Rabbit RPC circuit-breaker integration is not implemented.

Risks:
- [x] Retrying non-idempotent RPC calls can duplicate downstream side effects and is documented explicitly.
- [x] Timeout or cancellation may not stop broker-side or remote work and is documented explicitly.

## Phase 14.12 Documentation And Limitations

Goal:
- [x] Document milestone 14 honestly after implementation.

Implementation tasks:
- [x] Update README and usage docs with blocking bridge, hybrid mode, and migration support wording.
- [x] Document `RabbitTemplate` for event messaging only.
- [x] Document `AsyncRabbitTemplate` for RPC only.
- [x] Document that RPC does not use outbox by default.
- [x] Document fail-fast rejection as the only first-version event-bridge overload policy.
- [x] Document virtual threads as optimized blocking support, not reactive behavior.
- [x] Document Strategy A listener semantics and Strategy B as unsupported.
- [x] Document R2DBC flusher, schema resolution, PostgreSQL JSON binding, and PostgreSQL claim optimization.
- [x] Document Rabbit RPC raw/envelope modes, generic response types, retry, bulkhead, metrics, and limitations.
- [x] Document Rabbit RPC circuit breaker as not implemented.

Success criteria:
- [x] Documentation uses blocking bridge, hybrid mode, and migration support wording.
- [x] Documentation does not overclaim fully reactive or non-blocking RabbitMQ broker I/O behavior.
- [x] Documentation clearly separates event messaging from RPC.
- [x] Documentation keeps event outbox separate from normal RPC.

Out of scope:
- [x] New runtime features.
- [x] Strategy B async ack coordination.
- [x] Binary outbox payload persistence.
- [x] MySQL, Oracle, or SQL Server optimized claim strategies.
- [x] Rabbit RPC circuit breaker.
