# Milestone 14 Rabbit WebFlux Bridge Checklist

This milestone adds RabbitMQ support for WebFlux systems as a blocking bridge, hybrid mode, and migration support. It must not be described as fully reactive RabbitMQ support.

## Global Guardrails

- [ ] Keep event messaging and RPC in separate modules and APIs.
- [ ] Use `RabbitTemplate` for event messaging only.
- [ ] Use `AsyncRabbitTemplate` for RPC only.
- [ ] Do not use outbox for RPC by default.
- [ ] Do not add outbox integration to milestone 14 unless explicitly requested later.
- [ ] Do not hide blocking RabbitMQ calls behind `Mono` without an explicit bridge executor.
- [ ] Do not run blocking RabbitMQ work on Netty event-loop threads.
- [ ] Use bounded executors, bounded queues, and bounded concurrency.
- [ ] Implement only `fail-fast` rejection in the first version.
- [ ] Do not implement `block-caller` rejection yet.
- [ ] Do not implement `drop-and-metric` rejection yet.
- [ ] Ack only after the handler `Mono` completes successfully.
- [ ] Do not implement Strategy B async ack coordination yet.
- [ ] Treat virtual threads only as a blocking bridge executor option.
- [ ] Do not claim fully reactive, fully non-blocking, exactly-once, or unlimited concurrency semantics.

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
- [ ] Implement WebFlux-facing event publish through `RabbitTemplate`, isolated by the bridge executor and concurrency guard.

Implementation tasks:
- [ ] Add `ReactiveRabbitBridgePublisher`.
- [ ] Wire it as a `ReactiveReliablePublisher` implementation.
- [ ] Serialize messages using the existing reliable message format.
- [ ] Publish events with `RabbitTemplate`.
- [ ] Offload blocking publish work through the bridge executor.
- [ ] Guard publish calls with the concurrency guard.
- [ ] Ensure `AsyncRabbitTemplate` is not used.

Tests to write first:
- [ ] `publish(...)` calls `RabbitTemplate.convertAndSend`.
- [ ] Publish runs on the bridge executor, not the caller thread.
- [ ] Permit is released after successful publish.
- [ ] Permit is released after publish failure.
- [ ] Executor or guard saturation returns a `Mono` error.
- [ ] Serialization failure does not call `RabbitTemplate`.
- [ ] No `AsyncRabbitTemplate` dependency or usage exists in the event bridge module.

Success criteria:
- [ ] Event publish works through `RabbitTemplate` only.
- [ ] Blocking publish does not run inline on the subscriber thread.
- [ ] No outbox integration exists in this phase.

Risks:
- [ ] Blocking work is wrapped with `Mono.just(...)`.
- [ ] `subscribeOn` uses an uncontrolled scheduler.
- [ ] Event publishing accidentally gains RPC semantics.

Out of scope:
- [ ] Listener implementation.
- [ ] Retry and DLQ integration.
- [ ] RPC.
- [ ] Outbox persistence.

## Phase 14.5 Reactive Event Listener Bridge With Strategy A

Goal:
- [ ] Consume Rabbit messages with Spring AMQP listener infrastructure and invoke reactive handlers with simple ack-after-completion semantics.

Implementation tasks:
- [ ] Add `ReactiveRabbitBridgeListenerEndpoint`.
- [ ] Add `ReactiveRabbitBridgeListenerMethodInvoker`.
- [ ] Add `ReactiveRabbitBridgeListenerRegistrar`.
- [ ] Add `ReactiveRabbitBridgeMessageHandler`.
- [ ] Wire Spring AMQP listener container support.
- [ ] Invoke handler `Mono` and wait for completion using Strategy A.
- [ ] Ack only after handler `Mono` completes successfully.

Tests to write first:
- [ ] `@ReactiveReliableListener` method returning `Mono<Void>` is invoked.
- [ ] Non-public listener method can be invoked.
- [ ] Handler `Mono` is awaited before ack.
- [ ] Ack happens only after delayed `Mono` completion.
- [ ] Handler error does not ack as success.
- [ ] No Strategy B async ack coordination is present.

Success criteria:
- [ ] Strategy A listener bridge works.
- [ ] Ack-after-success is proven by tests.
- [ ] Blocking wait is explicit and isolated to the listener bridge boundary.

Risks:
- [ ] Ack happens before `Mono` completion.
- [ ] Listener blocks a thread that should not be blocked.
- [ ] Reactor Context is lost.

Out of scope:
- [ ] Strategy B async ack coordination.
- [ ] Retry and DLQ routing.
- [ ] RPC.
- [ ] Metrics.

## Phase 14.6 Listener Failure, Duplicate, And Idempotency Semantics

Goal:
- [ ] Add event messaging reliability semantics around reactive Rabbit consume flow.

Implementation tasks:
- [ ] Integrate `ReactiveIdempotencyStore`.
- [ ] Handle duplicate `SUCCESS` without invoking the handler.
- [ ] Handle duplicate `PROCESSING` and `FAILED` without acking as success.
- [ ] Mark success only after handler `Mono` completes.
- [ ] Mark failure when handler or idempotency flow fails.
- [ ] Route event failures through Rabbit-native retry and DLQ semantics.

Tests to write first:
- [ ] New message runs `tryStart`, handler `Mono`, `markSuccess`, then ack.
- [ ] Duplicate `SUCCESS` acks without invoking handler.
- [ ] Duplicate `PROCESSING` does not ack as success.
- [ ] Duplicate `FAILED` does not ack as success.
- [ ] Handler failure marks failure and does not ack as success.
- [ ] Idempotency store failure does not silently ack.
- [ ] Retry exhaustion routes to DLQ using event messaging semantics.

Success criteria:
- [ ] Duplicate behavior cannot silently lose messages.
- [ ] Failure behavior is explicit.
- [ ] Retry and DLQ stay event messaging concerns.

Risks:
- [ ] Reactor retry is mistaken for Rabbit business retry.
- [ ] Idempotency failure is treated as business success.
- [ ] Ack/nack behavior conflicts with broker retry configuration.

Out of scope:
- [ ] RPC.
- [ ] Outbox integration.
- [ ] Strategy B async ack coordination.
- [ ] Advanced observability.

## Phase 14.7 Event Loop Protection And Overload Behavior

Goal:
- [ ] Make unsafe bridge usage visible and keep overload behavior bounded.

Implementation tasks:
- [ ] Add event-loop detection helper.
- [ ] Warn or record a safety signal when publish is called from a Reactor HTTP event-loop thread.
- [ ] Ensure publish is still offloaded to the bridge executor.
- [ ] Ensure saturation fails fast.
- [ ] Ensure cancellation releases concurrency resources.

Tests to write first:
- [ ] Publish called from a Reactor HTTP-style thread name emits a warning or safety signal.
- [ ] Publish called from an event-loop-style thread still offloads blocking work.
- [ ] Saturated concurrency guard fails fast.
- [ ] Cancellation releases resources.
- [ ] No unbounded queue path exists.

Success criteria:
- [ ] Blocking calls remain isolated.
- [ ] Overload behavior is deterministic.
- [ ] Safety checks do not change successful business behavior.

Risks:
- [ ] Thread-name detection is imperfect.
- [ ] Safety checks become noisy.
- [ ] Protection logic becomes more complex than the bridge itself.

Out of scope:
- [ ] BlockHound integration.
- [ ] Load testing.
- [ ] Non-fail-fast rejection policies.

## Phase 14.8 Event Observability

Goal:
- [ ] Add bridge metrics and optional tracing signals for event publish and consume.

Implementation tasks:
- [ ] Add `RabbitBridgeMetrics`.
- [ ] Record publish success and failure.
- [ ] Record consume success, failure, duplicate, retry, and DLQ outcomes.
- [ ] Record executor active, queued, and rejected counts where available.
- [ ] Tag metrics with `runtime=webflux-bridge`.
- [ ] Tag metrics with `transport=rabbit`.
- [ ] Tag metrics with `executor_mode=platform|virtual-thread`.

Tests to write first:
- [ ] Publish success counter increments.
- [ ] Publish failure counter increments.
- [ ] Consume success counter increments.
- [ ] Consume failure counter increments.
- [ ] Duplicate counter increments.
- [ ] Executor rejection counter increments.
- [ ] Metrics include bridge runtime and Rabbit transport tags.

Success criteria:
- [ ] Operators can see bridge saturation and failures.
- [ ] Metrics do not change business flow.
- [ ] Metrics wording reflects blocking bridge and hybrid mode.

Risks:
- [ ] Metrics duplicate existing observability abstractions.
- [ ] Queue size gauges differ between platform and virtual-thread modes.

Out of scope:
- [ ] RPC metrics.
- [ ] Mandatory distributed tracing dependency.

## Phase 14.9 Rabbit RPC WebFlux Bridge Module Scaffold

Goal:
- [ ] Create a separate RPC bridge module using `AsyncRabbitTemplate`.

Implementation tasks:
- [ ] Add `reliable-message-rpc-rabbit-webflux-bridge/pom.xml`.
- [ ] Add the module to the root `pom.xml`.
- [ ] Add `ReactiveRabbitRpcClient`.
- [ ] Add `RabbitRpcWebFluxBridgeProperties`.
- [ ] Add `RabbitRpcWebFluxBridgeAutoConfiguration`.
- [ ] Keep package and class names clearly separate from event messaging.

Tests to write first:
- [ ] Auto-configuration creates RPC client only when `AsyncRabbitTemplate` exists.
- [ ] RPC auto-configuration does not create event publisher beans.
- [ ] RPC auto-configuration does not create event listener beans.
- [ ] RPC module does not depend on outbox by default.
- [ ] RPC module does not use `RabbitTemplate`.

Success criteria:
- [ ] RPC bridge is physically separate from the event bridge module.
- [ ] `AsyncRabbitTemplate` appears only in the RPC bridge.
- [ ] No outbox behavior exists in RPC defaults.

Risks:
- [ ] Shared configuration blurs event and RPC semantics.
- [ ] RPC code accidentally depends on event bridge classes.

Out of scope:
- [ ] Event messaging.
- [ ] Outbox.
- [ ] `RabbitTemplate`.
- [ ] Retry, circuit breaker, and bulkhead behavior.

## Phase 14.10 Rabbit RPC Request/Response Client

Goal:
- [ ] Implement Rabbit request/reply over `AsyncRabbitTemplate` with WebFlux-friendly `Mono` composition.

Implementation tasks:
- [ ] Add `DefaultReactiveRabbitRpcClient`.
- [ ] Use `AsyncRabbitTemplate` request/reply APIs.
- [ ] Convert the returned future to `Mono`.
- [ ] Apply timeout behavior.
- [ ] Propagate correlation ID and RPC headers.
- [ ] Surface remote failure, timeout, broker failure, and deserialization failure.

Tests to write first:
- [ ] Request sends through `AsyncRabbitTemplate`.
- [ ] Successful future completes the returned `Mono`.
- [ ] Timeout fails the returned `Mono`.
- [ ] Remote failure propagates.
- [ ] Reply deserialization failure propagates.
- [ ] Correlation ID and headers are sent.
- [ ] No outbox interaction occurs.
- [ ] No `RabbitTemplate` usage exists.

Success criteria:
- [ ] RPC request/reply works without blocking the WebFlux caller.
- [ ] Timeout is caller-visible.
- [ ] RPC remains request/response, not event messaging.

Risks:
- [ ] Timeout cancellation may not cancel broker-side or remote work.
- [ ] Request/reply retry can duplicate downstream side effects if used carelessly.

Out of scope:
- [ ] Event messaging.
- [ ] Durable command workflow.
- [ ] Circuit breaker.
- [ ] Audit.

## Phase 14.11 Rabbit RPC Retry, Circuit Breaker, Bulkhead, And Metrics

Goal:
- [ ] Add normal RPC resilience features without event outbox or DLQ semantics.

Implementation tasks:
- [ ] Reuse or adapt `ReactiveRpcOperator` if it fits.
- [ ] Add retry for retryable RPC failures.
- [ ] Add timeout metrics.
- [ ] Add retry metrics.
- [ ] Add circuit breaker integration if the existing RPC extension supports it.
- [ ] Add bulkhead behavior without unbounded queueing.
- [ ] Record request, success, failure, timeout, retry, and duration metrics.

Tests to write first:
- [ ] Retry happens for retryable timeout or connection errors.
- [ ] Non-retryable errors are not retried.
- [ ] Circuit breaker open fails fast.
- [ ] Bulkhead rejects without unbounded queueing.
- [ ] Metrics count request success.
- [ ] Metrics count failure.
- [ ] Metrics count timeout.
- [ ] Metrics count retry.
- [ ] RPC still does not use outbox by default.

Success criteria:
- [ ] RPC behavior matches request/response dependency semantics.
- [ ] RPC does not use Rabbit event retry queues or DLQ as its primary flow.
- [ ] RPC metrics are separate from event bridge metrics.

Risks:
- [ ] Retrying non-idempotent RPC calls creates duplicate side effects.
- [ ] Circuit breaker integration expands the milestone too much.
- [ ] Bulkhead queueing accidentally becomes unbounded.

Out of scope:
- [ ] Event messaging retry queues.
- [ ] Durable async command workflow.
- [ ] Outbox-backed RPC.

## Phase 14.12 Documentation And Limitations

Goal:
- [ ] Document milestone 14 honestly after behavior exists.

Implementation tasks:
- [ ] Update README with blocking bridge, hybrid mode, and migration support wording.
- [ ] Add Rabbit WebFlux bridge usage docs.
- [ ] Document `RabbitTemplate` for event messaging only.
- [ ] Document `AsyncRabbitTemplate` for RPC only.
- [ ] Document that RPC does not use outbox by default.
- [ ] Document fail-fast rejection as the only first-version rejection policy.
- [ ] Document virtual threads as a blocking bridge executor option.
- [ ] Document limitations and non-goals.

Tests to write first:
- [ ] Verify documentation examples match actual class and property names.
- [ ] Verify docs do not mention full reactive RabbitMQ support.
- [ ] Verify docs do not recommend `AsyncRabbitTemplate` for event publishing.
- [ ] Verify docs do not recommend outbox for normal RPC.

Success criteria:
- [ ] Documentation uses blocking bridge, hybrid mode, and migration support wording.
- [ ] Documentation does not overclaim full reactive or fully non-blocking RabbitMQ behavior.
- [ ] Documentation clearly separates event messaging from RPC.

Risks:
- [ ] Docs overclaim implementation guarantees.
- [ ] Docs hide blocking limitations.
- [ ] Docs blur event and RPC semantics.

Out of scope:
- [ ] New runtime features.
- [ ] Strategy B async ack documentation as supported behavior.
- [ ] Outbox integration docs for milestone 14.
