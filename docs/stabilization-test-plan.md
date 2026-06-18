# Stabilization Test Plan

This plan defines stabilization coverage for the current Reliable Message architecture and Milestone 14 Rabbit WebFlux direction. It is a test plan only; it does not introduce new runtime behavior.

## Guardrails

- Keep event messaging and RPC separate.
- Use `RabbitTemplate` for Rabbit event messaging only.
- Use `AsyncRabbitTemplate` for Rabbit RPC only.
- Do not add outbox behavior to normal RPC flows.
- Do not document or test binary payload storage as implemented yet.
- Treat the Rabbit WebFlux event bridge as a blocking bridge, hybrid mode, and migration support path.
- Treat virtual threads as a blocking optimization, not as reactive RabbitMQ or unlimited concurrency.
- Keep overload behavior bounded and fail-fast where currently implemented.

## Test Type Legend

- Unit: verifies one class or narrow behavior without external infrastructure.
- Integration: verifies module behavior with Spring context and, where needed, Testcontainers or test infrastructure.
- Sample-app smoke: verifies a small runnable application configuration boots and exercises the documented usage path.

## S1 Build & Sample Boot - Done

Goal:
- Prove the repository builds and representative MVC/WebFlux application contexts can boot with current auto-configuration.

Sample app/module involved:
- Root Maven reactor.
- Proposed sample smoke fixtures for MVC Rabbit, MVC Kafka, WebFlux Kafka, Rabbit WebFlux blocking bridge, and Rabbit RPC WebFlux.
- Existing modules: `reliable-message-mvc-starter`, `reliable-message-webflux-starter`, `reliable-message-rabbit-webflux-bridge`, `reliable-message-rpc-rabbit-webflux-bridge`.

Setup:
- Run full Maven test/build from the repository root.
- Boot each sample fixture with only the dependencies and properties documented for that stack.
- Use mocked or container-backed broker/storage dependencies depending on the fixture purpose.

Test cases:
- Unit: none required for this phase.
- Integration: full reactor test run.
- Sample-app smoke: MVC Rabbit sample context loads.
- Sample-app smoke: MVC Kafka sample context loads.
- Sample-app smoke: WebFlux Kafka sample context loads.
- Sample-app smoke: Rabbit WebFlux blocking bridge sample context loads.
- Sample-app smoke: Rabbit RPC WebFlux sample context loads only when an `AsyncRabbitTemplate` bean is present.

Expected result:
- Build succeeds.
- Each sample context loads with expected beans.
- RPC sample does not create event publisher/listener beans.
- Event bridge samples do not require RPC beans.

Failure cases:
- Missing auto-configuration imports.
- Ambiguous beans are silently ignored instead of failing clearly.
- Sample docs reference classes or properties that do not exist.
- RPC sample accidentally requires outbox or event bridge beans.

Pass criteria:
- Full build passes.
- All smoke fixtures boot.
- Bean presence/absence matches the selected stack.
- No sample uses unsupported binary payload storage.

Out of scope:
- End-to-end broker delivery assertions.
- Performance testing.
- Adding production sample applications.

## S2 MVC Rabbit - Done

Goal:
- Stabilize MVC Rabbit event messaging with publish, consume, idempotency, outbox, and Rabbit-native retry/DLQ behavior.

Sample app/module involved:
- `reliable-message-rabbit-mvc`.
- `reliable-message-mvc-starter`.
- `reliable-message-outbox-jdbc`.
- `reliable-message-idempotency-jdbc` or `reliable-message-idempotency-redis`.
- Proposed MVC Rabbit sample-app smoke fixture.

Setup:
- Start RabbitMQ for integration tests.
- Use a relational database for JDBC outbox/idempotency paths when those paths are enabled.
- Configure `message.reliability.transport=rabbit`.
- Provision Rabbit exchange, queues, bindings, retry, and DLQ topology according to the MVC Rabbit module behavior.

Test cases:
- Unit: publisher serializes and delegates to Rabbit event publishing path.
- Unit: listener invocation handles valid and invalid payloads.
- Integration: `ReliablePublisher.publish(...)` sends a reliable event envelope.
- Integration: `@ReliableListener` consumes and acks after successful handler completion.
- Integration: duplicate `SUCCESS` idempotency state skips handler and acks.
- Integration: duplicate `PROCESSING` or `FAILED` does not ack as success.
- Integration: handler failure triggers failure marking and Rabbit failure path.
- Integration: JDBC outbox row is published by the MVC outbox flusher and marked published only after successful broker publish.
- Sample-app smoke: publish an event through an HTTP endpoint and observe one listener-side business invocation.

Expected result:
- MVC Rabbit event flow is at-least-once with idempotency-based effectively-once handler protection.
- Outbox applies to event messaging only.
- Failed events follow configured Rabbit retry/DLQ behavior.

Failure cases:
- Ack before handler success.
- Duplicate event invokes handler more than intended.
- Outbox marks published before successful publish.
- Retry/DLQ topology is missing or misrouted.
- RPC abstractions appear in MVC Rabbit event tests.

Pass criteria:
- All MVC Rabbit unit and integration tests pass.
- Sample smoke confirms publish/consume path.
- No `AsyncRabbitTemplate` is used for event publishing.

Out of scope:
- RPC request/response.
- WebFlux Rabbit blocking bridge.
- Binary payload storage.

## S3 WebFlux Rabbit Blocking Bridge - Done

Goal:
- Stabilize Rabbit event messaging for WebFlux through the blocking bridge, including executor isolation, fail-fast overload, listener Strategy A, idempotency, failure propagation, and metrics.

Sample app/module involved:
- `reliable-message-rabbit-webflux-bridge`.
- `reliable-message-webflux-starter`.
- `reliable-message-idempotency-redis-reactive` or `reliable-message-idempotency-r2dbc`.
- Optional `reliable-message-outbox-r2dbc`.
- Proposed Rabbit WebFlux blocking bridge sample-app smoke fixture.

Setup:
- Start RabbitMQ.
- Configure `message.reliability.transport=rabbit`.
- Run separate profiles for platform executor mode and virtual-thread executor mode.
- Provision Rabbit event topology or enable supported event topology declaration.
- Provide a reactive idempotency store for listener reliability tests.
- Broker retry/DLQ routing is infrastructure-owned; this phase validates bridge failure propagation, nack behavior, and failure-hook outcomes only.

Test cases:
- Unit: `ReactiveRabbitBridgePublisher` serializes before submitting bridge work.
- Unit: publish work runs on the bridge executor, not the caller thread.
- Unit: saturation fails fast with the bridge rejection exception.
- Unit: event-loop-style caller emits a warning or safety signal while still offloading blocking publish.
- Unit: cancellation releases bridge resources without running `RabbitTemplate` inline.
- Integration: `ReactiveReliablePublisher.publish(...)` calls `RabbitTemplate.convertAndSend` on bridge executor.
- Integration: `@ReactiveReliableListener` `Mono<Void>` handler is invoked.
- Integration: delayed handler `Mono` delays ack.
- Integration: ack happens only after handler `Mono` completes and idempotency `markSuccess` succeeds.
- Integration: handler failure calls `markFailed` and nacks or invokes the failure path, not success ack.
- Integration: failure-hook retry/DLQ outcomes are observable when a hook reports them; transport retry/DLQ routing remains external infrastructure.
- Integration: duplicate `SUCCESS` acks and skips handler.
- Integration: duplicate `PROCESSING` and `FAILED` do not ack as success.
- Integration: platform and virtual-thread modes both enforce `max-concurrency`.
- Sample-app smoke: WebFlux endpoint publishes an event through auto-configured bridge beans and listener processes it once in platform and virtual-thread executor modes.

Expected result:
- Blocking Rabbit event work is isolated behind a dedicated bridge executor.
- Listener Strategy A blocks only at the bridge boundary.
- No blocking Rabbit work runs on Netty event-loop threads.
- Metrics include bridge runtime, Rabbit transport, executor mode, event name, and status tags where implemented.

Failure cases:
- `RabbitTemplate` runs inline on caller/event-loop thread.
- Ack happens before handler `Mono` completion.
- Permit leaks after cancellation, failure, or rejection.
- Virtual-thread mode bypasses `max-concurrency`.
- `AsyncRabbitTemplate` appears in event bridge code.

Pass criteria:
- Unit, integration, and smoke tests pass for platform and virtual-thread modes.
- Event bridge behavior is documented and observed as blocking bridge / hybrid mode.
- No RPC behavior appears in event bridge tests.
- Broker-backed smoke uses the auto-configured `ReactiveRabbitBridgeListenerRegistrar` path.
- S3 is complete for failure propagation; bridge-owned retry/DLQ transport routing is not claimed.

Out of scope:
- Strategy B async ack coordination.
- Rabbit RPC.
- Bridge-owned retry/DLQ transport routing or topology creation.
- Binary payload storage.

## S4 Rabbit RPC WebFlux - Done

Goal:
- Stabilize Rabbit request/response over `AsyncRabbitTemplate` with WebFlux-friendly `Mono` boundaries, bounded executor modes, retry, envelope errors, and RPC-specific metrics.

Sample app/module involved:
- `reliable-message-rpc-rabbit-webflux-bridge`.
- `reliable-message-rpc-core`.
- Proposed Rabbit RPC WebFlux sample-app smoke fixture.

Setup:
- Start RabbitMQ.
- Provide an `AsyncRabbitTemplate` bean backed by a `RabbitTemplate` configured with a `SmartMessageConverter`.
- Provision the RPC exchange, responder queue, and route bindings.
- Configure platform and virtual-thread RPC executor profiles.

Test cases:
- Unit: request execution is lazy until subscription.
- Unit: `AsyncRabbitTemplate.convertSendAndReceiveAsType(...)` is invoked on the RPC bridge executor, not the caller thread.
- Unit: Class and `ParameterizedTypeReference<T>` response paths work.
- Unit: raw response mode remains compatible.
- Unit: envelope `SUCCESS` returns payload.
- Unit: envelope `ERROR` maps to `RabbitRpcRemoteException`.
- Unit: timeout is caller-visible and does not block.
- Unit: cancellation cancels client-side future where possible without claiming broker-side cancellation.
- Unit: retry is bounded and only applies to configured retryable timeout or transient failures.
- Unit: remote `ERROR` envelope and conversion failures are not retried by default.
- Unit: bulkhead saturation fails fast.
- Unit: permits release on success, failure, timeout, cancellation, and executor rejection.
- Integration: successful request/reply round trip through RabbitMQ.
- Integration: timeout path when no reply arrives.
- Integration: retry uses fresh physical AMQP correlation ID per attempt while preserving logical correlation headers.
- Integration: RPC metrics record request, success, failure, timeout, retry, bulkhead rejection, and duration.
- Sample-app smoke: WebFlux endpoint calls `ReactiveRabbitRpcClient` and returns response.

Expected result:
- RPC remains request/response semantics.
- RPC does not use outbox or Rabbit event retry/DLQ.
- `AsyncRabbitTemplate` is isolated to the RPC bridge module.
- `RabbitTemplate` is not used directly by RPC client implementation.

Failure cases:
- RPC send path runs inline on WebFlux caller thread.
- Retry reuses physical AMQP correlation ID and accepts stale replies.
- Remote application errors are retried as if they were broker failures.
- RPC metrics use event bridge metric names.
- RPC auto-configuration creates event publisher/listener beans.

Pass criteria:
- RPC unit and integration tests pass.
- Sample RPC app boots and completes a request/reply call.
- Boundary scans confirm no event publisher/listener/outbox usage in RPC module.

Out of scope:
- Rabbit event publishing.
- Outbox-backed RPC.
- Rabbit event retry queue or DLQ behavior.
- Durable async command workflow.

## S5 R2DBC Outbox - Done

Goal:
- Stabilize reactive event outbox storage, schema configuration, flush scheduling, and claim behavior for WebFlux event publishers.

Sample app/module involved:
- `reliable-message-outbox-r2dbc`.
- `reliable-message-kafka-webflux`.
- `reliable-message-rabbit-webflux-bridge`.
- Proposed WebFlux Kafka and Rabbit WebFlux outbox smoke fixtures.

Setup:
- Start R2DBC-supported databases for target integration tests.
- At minimum, cover PostgreSQL for JSON binding and optimized claim strategy.
- Provision `message_outbox` schema with migrations before enabling the flusher.
- Configure `message.reliability.outbox.enabled=true` and `message.reliability.outbox.flush-enabled=true`.
- Provide a `ReactiveReliablePublisher` bean.

Test cases:
- Unit: outbox properties bind and validate flush interval, batch size, concurrency, retry delay, and timeout.
- Unit: schema resolver uses explicit config before dialect defaults, then generic fallback.
- Unit: `payload-storage=text` resolves text-like columns.
- Unit: `payload-storage=json` resolves dialect JSON where supported and text fallback where documented.
- Unit: `payload-storage=binary` fails clearly as not implemented.
- Unit: PostgreSQL JSON binder uses dialect-aware JSON binding without SQL casts.
- Unit: generic claim strategy preserves select-ID plus conditional-update behavior.
- Unit: PostgreSQL claim strategy uses `FOR UPDATE SKIP LOCKED` without window functions.
- Integration: save and read text payload row.
- Integration: save and read PostgreSQL JSON payload/header row.
- Integration: flusher reads claimed rows and publishes through `ReactiveReliablePublisher`.
- Integration: `markPublished` happens only after publish success.
- Integration: `markFailed` records failure and next retry after publish error.
- Integration: overlapping flush ticks are skipped.
- Integration: concurrent PostgreSQL claimers do not receive the same row.
- Sample-app smoke: WebFlux Kafka or Rabbit bridge fixture flushes one outbox row through the active publisher.

Expected result:
- R2DBC outbox is event messaging only.
- Flusher does not call Kafka or Rabbit clients directly.
- PostgreSQL optimized claiming reduces duplicate claim contention.
- Generic fallback is tested only against databases whose SQL syntax is supported by the current fallback.

Failure cases:
- Flusher starts without `ReactiveReliablePublisher`.
- Row is marked published before publish success.
- One item failure cancels unrelated in-flight batch items.
- Store or publish hang permanently blocks future flushes without timeout handling.
- Binary payload storage is accepted as if implemented.

Pass criteria:
- R2DBC outbox unit and integration tests pass.
- PostgreSQL schema, binding, and claim tests pass.
- Sample smoke confirms durable event row flush.
- RPC module does not create or use the outbox flusher.

Out of scope:
- Binary payload read/write.
- Payload compression or protobuf.
- MySQL, Oracle, or SQL Server optimized claim strategies unless implemented later.
- RPC outbox.

## S6 Idempotency Retry DLQ

Goal:
- Stabilize duplicate detection, retry, and DLQ behavior for event messaging across MVC and WebFlux stacks without mixing in RPC semantics.

Sample app/module involved:
- `reliable-message-idempotency-jdbc`.
- `reliable-message-idempotency-redis`.
- `reliable-message-idempotency-r2dbc`.
- `reliable-message-idempotency-redis-reactive`.
- Rabbit MVC, Rabbit WebFlux bridge, Kafka MVC, and Kafka WebFlux event modules.

Setup:
- Start required stores: relational database and/or Redis.
- Start RabbitMQ and Kafka where broker-specific retry/DLQ behavior is tested.
- Configure event listeners with idempotency providers.
- Provision retry/DLQ or retry/DLT topology according to the transport under test.

Test cases:
- Unit: MVC idempotency store transitions new key to `PROCESSING`, then `SUCCESS` or `FAILED`.
- Unit: reactive idempotency store returns duplicate state without null/NPE behavior.
- Integration: duplicate `SUCCESS` skips handler and completes broker ack/commit.
- Integration: duplicate `PROCESSING` does not ack/commit as success.
- Integration: duplicate `FAILED` does not ack/commit as success.
- Integration: handler failure marks failed before broker retry/DLQ path.
- Integration: Rabbit listener failure path nacks or invokes failure hook according to configured event behavior.
- Integration: Kafka listener failure path does not commit offset as success.
- Integration: retry/DLQ outcome metrics or hooks are emitted only when concrete event outcomes exist.

Expected result:
- Event handling is at-least-once with idempotency protection.
- Retry/DLQ is transport event behavior, not RPC retry behavior.
- Duplicate state handling is explicit and observable.

Failure cases:
- Duplicate `PROCESSING` is acked/committed as success.
- `markFailed` failure masks the original listener failure.
- Deserialization failure bypasses event failure hook where hook is expected.
- Retry/DLQ hook is applied to RPC.

Pass criteria:
- Store transition tests pass for supported MVC and reactive stores.
- Broker-specific event failure tests pass.
- No RPC tests depend on event retry/DLQ behavior.

Out of scope:
- New retry topology design.
- Strategy B async Rabbit ack coordination.
- RPC retry and circuit breaker behavior.

## S7 Observability Metrics

Goal:
- Stabilize metrics and observability signals without changing business flow.

Sample app/module involved:
- `reliable-message-observability`.
- `reliable-message-rabbit-webflux-bridge`.
- `reliable-message-rpc-rabbit-webflux-bridge`.
- `reliable-message-outbox-r2dbc`.
- Rabbit/Kafka MVC and WebFlux event modules where metrics exist.

Setup:
- Use `SimpleMeterRegistry` or application-managed test `MeterRegistry`.
- Run tests with one unique primary registry when multiple registries exist.
- Exercise success, failure, duplicate, retry/DLQ, rejection, timeout, and duration paths.

Test cases:
- Unit: event publish success and failure counters increment.
- Unit: event consume success and failure counters increment.
- Unit: duplicate counters increment with outcome tags.
- Unit: bridge executor rejection counter increments.
- Unit: platform executor active/queued gauges are registered where available.
- Unit: virtual-thread mode does not falsely expose platform queue gauges.
- Unit: RPC request, success, failure, timeout, retry, bulkhead rejection, and duration metrics increment.
- Unit: RPC metrics are separate from Rabbit event bridge metrics.
- Integration: metrics preserve original error propagation.
- Integration: ambiguous `MeterRegistry` wiring fails clearly or follows implemented fail-fast behavior.
- Sample-app smoke: scrape or inspect registry after one publish, one consume, and one RPC call.

Expected result:
- Metrics never convert failures into success.
- Tags include runtime, transport, executor mode, event name or route, and status where applicable.
- Event metrics and RPC metrics use separate names and semantics.

Failure cases:
- Metrics are silently disabled when ambiguous registries exist.
- Metrics allocation creates hidden in-memory registry.
- RPC timeout later succeeds by retry but timeout attempt is not counted.
- Metrics code swallows business errors.

Pass criteria:
- Metrics unit and integration tests pass.
- Sample smoke confirms expected meter names and tags.
- No mandatory tracing dependency is introduced.

Out of scope:
- Full distributed tracing.
- New metrics backend configuration.
- Performance benchmarking.

## S8 MVC Kafka

Goal:
- Stabilize MVC Kafka event messaging with publish, consume, partition keys, idempotency, and Kafka retry/DLT behavior where implemented.

Sample app/module involved:
- `reliable-message-kafka-mvc`.
- `reliable-message-mvc-starter`.
- `reliable-message-outbox-jdbc`.
- `reliable-message-idempotency-jdbc` or `reliable-message-idempotency-redis`.
- Proposed MVC Kafka sample-app smoke fixture.

Setup:
- Start Kafka.
- Configure `message.reliability.transport=kafka`.
- Configure topic prefix and listener topics.
- Configure idempotency and optional JDBC outbox.
- Provision retry/DLT topics if required by current Kafka behavior.

Test cases:
- Unit: `ReliablePublisher.publish(...)` maps `PublishOptions.partitionKey` to Kafka record key.
- Unit: reliable envelope serialization is stable.
- Integration: MVC publish sends to expected Kafka topic.
- Integration: listener invokes `@ReliableListener` once for a new event.
- Integration: successful handler commits offset after idempotency success.
- Integration: handler failure does not commit as success.
- Integration: duplicate `SUCCESS` commits/skips handler.
- Integration: JDBC outbox flush publishes pending Kafka event and marks published after send success.
- Sample-app smoke: HTTP endpoint publishes Kafka event and consumer processes it.

Expected result:
- Kafka MVC event flow remains event messaging with eventual consistency.
- Partition key is honored.
- Event retry/DLT behavior stays separate from RPC retry.

Failure cases:
- Offset commits before handler/idempotency success.
- Partition key is ignored.
- Outbox marks published before Kafka send success.
- Rabbit-specific classes appear in Kafka tests.

Pass criteria:
- MVC Kafka unit and integration tests pass.
- Sample smoke verifies publish/consume path.
- No Rabbit RPC behavior appears.

Out of scope:
- WebFlux Kafka.
- Rabbit retry/DLQ.
- New Kafka retry topology redesign.

## S9 WebFlux Kafka

Goal:
- Stabilize reactive Kafka event messaging with `ReactiveReliablePublisher`, reactive listener flow, R2DBC outbox, idempotency, and bounded processing.

Sample app/module involved:
- `reliable-message-kafka-webflux`.
- `reliable-message-webflux-starter`.
- `reliable-message-outbox-r2dbc`.
- `reliable-message-idempotency-r2dbc` or `reliable-message-idempotency-redis-reactive`.
- Proposed WebFlux Kafka sample-app smoke fixture.

Setup:
- Start Kafka.
- Start R2DBC database or Redis depending on idempotency provider.
- Provision `message_outbox` schema before enabling outbox flushing.
- Configure `message.reliability.transport=kafka`.
- Configure bounded listener prefetch/concurrency values.

Test cases:
- Unit: invalid prefetch/concurrency values fail clearly or are clamped according to implementation.
- Unit: reactive publisher does not use blocking JDBC.
- Unit: listener semaphore or concurrency guard releases only after successful acquire.
- Integration: `ReactiveReliablePublisher.publish(...)` sends event through Kafka reactive publisher.
- Integration: reactive listener invokes `@ReactiveReliableListener` returning `Mono<Void>`.
- Integration: offset commit happens only after handler `Mono` and idempotency success complete.
- Integration: duplicate `SUCCESS` skips handler and commits.
- Integration: duplicate `PROCESSING` and `FAILED` do not commit as success.
- Integration: R2DBC outbox flusher publishes pending rows through Kafka `ReactiveReliablePublisher`.
- Integration: bounded processing avoids unbounded `flatMap` or queues.
- Sample-app smoke: WebFlux endpoint writes outbox row or publishes event, Kafka consumer processes it once.

Expected result:
- WebFlux Kafka remains reactive where Kafka client support is reactive.
- R2DBC is used for reactive outbox, not JDBC.
- Offset commit follows successful processing.

Failure cases:
- Unbounded `flatMap` in listener or flusher.
- JDBC access inside reactive flow.
- Offset commit skips failed or duplicate-in-progress records.
- R2DBC outbox flusher publishes directly through Kafka client instead of `ReactiveReliablePublisher`.

Pass criteria:
- WebFlux Kafka unit and integration tests pass.
- Sample smoke verifies reactive publish/consume and optional outbox flush.
- No Rabbit bridge or RPC classes are required.

Out of scope:
- Rabbit WebFlux blocking bridge.
- RPC.
- Binary payload storage.

## S10 Docs Example Verification

Goal:
- Prove user-facing examples are accurate, copyable, and do not overclaim behavior.

Sample app/module involved:
- `docs/examples/mvc-rabbit.md`.
- `docs/examples/mvc-kafka.md`.
- `docs/examples/webflux-kafka.md`.
- `docs/examples/rabbit-webflux-bridge.md`.
- `docs/examples/rabbit-rpc-webflux.md`.
- `docs/examples/audit-extension.md`.
- Proposed docs-example smoke fixtures generated from the documented snippets.

Setup:
- Extract or mirror each example into a minimal smoke fixture.
- Use only documented dependencies, required infrastructure beans, and properties.
- Provision broker topology and database schema where the docs say it is required.
- Run markdown checks and stale-claim searches.

Test cases:
- Sample-app smoke: MVC Rabbit example boots and sends/consumes one event.
- Sample-app smoke: MVC Kafka example boots and sends/consumes one event.
- Sample-app smoke: WebFlux Kafka example boots and uses reactive publisher/listener.
- Sample-app smoke: Rabbit WebFlux blocking bridge example boots in platform mode.
- Sample-app smoke: Rabbit WebFlux blocking bridge example boots in virtual-thread mode.
- Sample-app smoke: Rabbit RPC WebFlux example boots with documented `AsyncRabbitTemplate`, smart converter, and RPC topology.
- Sample-app smoke: audit extension example boots with custom sink or no-op-safe configuration.
- Documentation check: no examples claim fully reactive RabbitMQ.
- Documentation check: no examples claim exactly-once delivery.
- Documentation check: no examples claim RPC uses outbox by default.
- Documentation check: no examples document binary payload storage as implemented.

Expected result:
- Every documented example either boots as written or clearly labels infrastructure prerequisites.
- Unsupported features are omitted or marked planned/unsupported.
- Anti-pattern guidance remains consistent across docs.

Failure cases:
- Example references a class, module, property, or bean that does not exist.
- Example enables R2DBC outbox without schema provisioning guidance.
- Rabbit RPC example omits `AsyncRabbitTemplate`, smart converter, or topology prerequisites.
- Docs imply `RabbitTemplate` is safe to call directly in WebFlux handlers.

Pass criteria:
- All docs-example smoke fixtures pass.
- Markdown validation passes.
- Stale-claim search finds no unsupported guarantee wording.
- Docs remain concise and production-oriented.

Out of scope:
- Rewriting all documentation.
- Adding runtime features to make examples work.
- Benchmarking or load testing.

