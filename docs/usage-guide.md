# Reliable Message Usage Guide

This guide explains how to adopt Reliable Message in an existing Spring Boot service and how to choose the current stack without blurring event messaging, RPC and audit semantics.

Reliable Message provides effectively-once message processing patterns through outbox, idempotency, retry/DLQ conventions and observability. It is not an exactly-once messaging framework.

## Choosing The Correct Stack

| Need | Use | Avoid |
| --- | --- | --- |
| Blocking Spring MVC service with RabbitMQ | MVC starter + Rabbit MVC adapter | WebFlux bridge unless the app is WebFlux. |
| Blocking Spring MVC service with Kafka | MVC starter + Kafka MVC adapter | Hiding Kafka semantics behind Rabbit assumptions. |
| Reactive WebFlux service with Kafka | WebFlux starter + Kafka WebFlux adapter | JDBC or blocking Redis in reactive flow. |
| WebFlux service that must use RabbitMQ | Rabbit WebFlux blocking bridge | Calling `RabbitTemplate` on event-loop threads. |
| Request/response over RabbitMQ | Rabbit RPC bridge direction with `AsyncRabbitTemplate` | Event outbox and DLQ as normal RPC flow. |
| Compliance capture | Audit extension | Treating observability logs as audit records. |

Current positioning:

- MVC + RabbitMQ and MVC + Kafka use blocking adapters.
- WebFlux + Kafka is the reactive messaging path.
- WebFlux + RabbitMQ uses a blocking bridge, hybrid mode and migration support.
- Rabbit RPC is separate request/response support and does not use outbox by default.

## Add Dependencies

MVC RabbitMQ:

```xml
<dependency>
  <groupId>io.github.huynhngochuyhoang</groupId>
  <artifactId>reliable-message-mvc-starter</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
```

MVC Kafka:

```xml
<dependency>
  <groupId>io.github.huynhngochuyhoang</groupId>
  <artifactId>reliable-message-mvc-starter</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
<dependency>
  <groupId>io.github.huynhngochuyhoang</groupId>
  <artifactId>reliable-message-kafka-mvc</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
```

WebFlux Kafka:

```xml
<dependency>
  <groupId>io.github.huynhngochuyhoang</groupId>
  <artifactId>reliable-message-webflux-starter</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
<dependency>
  <groupId>io.github.huynhngochuyhoang</groupId>
  <artifactId>reliable-message-kafka-webflux</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
```

WebFlux RabbitMQ blocking bridge:

```xml
<dependency>
  <groupId>io.github.huynhngochuyhoang</groupId>
  <artifactId>reliable-message-rabbit-webflux-bridge</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
```

## Configure Messaging

MVC RabbitMQ example:

```yaml
message:
  reliability:
    runtime: mvc
    transport: rabbit
    service-name: order-service
    rabbit:
      exchange: app.events
      auto-declare: true
      publisher-confirm: true
      listener-auto-startup: true
    retry:
      attempts: 5
      backoff:
        - 5s
        - 30s
        - 1m
        - 5m
    idempotency:
      ttl: 24h
```

MVC Kafka example:

```yaml
message:
  reliability:
    runtime: mvc
    transport: kafka
    service-name: order-service
    kafka:
      topic-prefix: app.
      consumer-group: order-service
      auto-declare: true
      listener-auto-startup: true
      partitions: 1
      replication-factor: 1
      publish-timeout: 5s
    retry:
      attempts: 5
      backoff:
        - 5s
        - 30s
        - 1m
        - 5m
    idempotency:
      ttl: 24h
```

JDBC outbox example:

```yaml
message:
  reliability:
    outbox:
      initialize-schema: true
      flush-enabled: true
      batch-size: 100
      flush-delay: 5s
      retry-delay: 30s
```

WebFlux Kafka example:

```yaml
message:
  reliability:
    runtime: webflux
    transport: kafka
    service-name: order-service
    kafka:
      topic-prefix: app.
      consumer-group: order-service
      producer-properties:
        bootstrap.servers: localhost:9092
      consumer-properties:
        bootstrap.servers: localhost:9092
    reactive:
      max-concurrency: 64
      prefetch: 256
```

WebFlux RabbitMQ blocking bridge example:

```yaml
message:
  reliability:
    runtime: webflux
    transport: rabbit
    service-name: order-service
    rabbit:
      exchange: app.events
      auto-declare: true
      bridge:
        enabled: true
        executor-mode: platform
        worker-threads: 16
        queue-capacity: 1000
        max-concurrency: 256
        rejection-policy: fail-fast
```

For Java 21 virtual-thread optimized blocking support:

```yaml
message:
  reliability:
    rabbit:
      bridge:
        executor-mode: virtual-thread
        queue-capacity: 1000
        max-concurrency: 1000
        rejection-policy: fail-fast
```

Virtual threads reduce blocking cost. They are not reactive and do not remove concurrency limits.

## MVC + RabbitMQ

Use MVC RabbitMQ when the application runtime is blocking.

Typical modules:

```text
reliable-message-mvc-starter
reliable-message-rabbit-mvc
reliable-message-outbox-jdbc
reliable-message-idempotency-jdbc or reliable-message-idempotency-redis
```

Publish:

```java
publisher.publish(
    "order.created",
    event,
    PublishOptions.builder()
        .aggregateId(orderId)
        .idempotencyKey(eventId)
        .correlationId(correlationId)
        .build()
);
```

Consume:

```java
@ReliableListener("order.created")
public void handle(ReliableMessage<OrderCreatedEvent> message) {
    orderService.handle(message.payload());
}
```

Reliability flow:

```text
transactional business write
 -> JDBC outbox row
 -> flush job
 -> RabbitTemplate publish
 -> listener
 -> idempotency tryStart
 -> handler
 -> markSuccess
 -> ack
```

## MVC + Kafka

Use MVC Kafka for blocking services that publish and consume Kafka events.

Typical modules:

```text
reliable-message-mvc-starter
reliable-message-kafka-mvc
reliable-message-outbox-jdbc
reliable-message-idempotency-jdbc or reliable-message-idempotency-redis
```

Use `partitionKey` when ordering by aggregate matters:

```java
publisher.publish(
    "order.created",
    event,
    PublishOptions.builder()
        .aggregateId(orderId)
        .partitionKey(orderId)
        .idempotencyKey(eventId)
        .build()
);
```

Kafka commits offsets only after successful processing.

## WebFlux + Kafka

Use WebFlux Kafka for reactive messaging.

Typical modules:

```text
reliable-message-webflux-starter
reliable-message-kafka-webflux
reliable-message-outbox-r2dbc
reliable-message-idempotency-r2dbc or reliable-message-idempotency-redis-reactive
```

R2DBC outbox flushing is opt-in with `message.reliability.outbox.enabled=true`. See [Milestone 14.8.1 R2DBC outbox flusher](milestone-14-8-1-r2dbc-outbox-flusher.md).

### R2DBC Outbox Schema Configuration

The R2DBC outbox can resolve payload, header, payload-bytes, and error column types from configuration and database dialect. Resolution order is:

1. User explicit config.
2. Dialect recommended default.
3. Generic fallback.

Storage mode:

| Property | Values | Default | Notes |
| --- | --- | --- | --- |
| `message.reliability.outbox.schema.payload-storage` | `text`, `json`, `binary` | `text` | `text` and `json` are supported today. `binary` is planned and fails fast until runtime binary codec/storage support is implemented. |

Advanced overrides:

| Property | Purpose |
| --- | --- |
| `message.reliability.outbox.schema.payload-column-type` | Overrides the `payload` column type. |
| `message.reliability.outbox.schema.headers-column-type` | Overrides the `headers` column type. |
| `message.reliability.outbox.schema.payload-bytes-column-type` | Overrides the `payload_bytes` column type. |
| `message.reliability.outbox.schema.last-error-column-type` | Overrides the `last_error` column type. |

Dialect defaults:

| Dialect | text payload | json payload/headers | planned binary payload bytes | last_error |
| --- | --- | --- | --- | --- |
| PostgreSQL | `text` | `jsonb` | `bytea` | `text` |
| MySQL | `longtext` | `json` | `longblob` | `longtext` |
| Oracle | `clob` | `clob` | `blob` | `clob` |
| SQL Server | `nvarchar(max)` | `nvarchar(max)` | `varbinary(max)` | `nvarchar(max)` |
| Generic fallback | `text` | `text` | `blob` | `text` |

PostgreSQL JSON example:

```yaml
message:
  reliability:
    outbox:
      enabled: true
      schema:
        payload-storage: json
```

SQL Server explicit override example:

```yaml
message:
  reliability:
    outbox:
      enabled: true
      schema:
        payload-storage: text
        payload-column-type: nvarchar(max)
        headers-column-type: nvarchar(max)
        last-error-column-type: nvarchar(max)
```

Binary mode planned example (fails fast today):

```yaml
message:
  reliability:
    outbox:
      enabled: true
      schema:
        payload-storage: binary
        payload-bytes-column-type: bytea
```

Binary mode is not supported by the current runtime store. Configuring `payload-storage: binary` fails fast with a clear startup error until a compatible payload codec and `payload_bytes` read/write path are implemented.

Publish:

```java
return reactivePublisher.publish(
    "order.created",
    event,
    PublishOptions.builder()
        .aggregateId(orderId)
        .partitionKey(orderId)
        .idempotencyKey(eventId)
        .correlationId(correlationId)
        .build()
);
```

Consume:

```java
@ReactiveReliableListener("order.created")
public Mono<Void> handle(ReliableMessage<OrderCreatedEvent> message) {
    return orderService.handle(message.payload());
}
```

Rules:

- Use R2DBC for database work.
- Use Reactive Redis or R2DBC for idempotency.
- Avoid unbounded `flatMap` and unbounded queues.
- Commit offsets only after the handler `Mono` completes successfully.

## WebFlux + RabbitMQ Blocking Bridge

Use the Rabbit WebFlux bridge when a WebFlux service must integrate with RabbitMQ through Spring AMQP.

Module:

```text
reliable-message-rabbit-webflux-bridge
```

This is a blocking bridge and hybrid mode. It provides a reactive API boundary, but `RabbitTemplate` and Spring AMQP listener infrastructure remain blocking.

Use it when:

```text
service is WebFlux
RabbitMQ is required by platform or migration constraints
blocking Rabbit work can be isolated on a bridge executor
bounded overload behavior is acceptable
```

Do not use it to claim fully reactive RabbitMQ, native Reactor RabbitMQ, or non-blocking broker I/O.

Start with the dedicated usage page:

[Rabbit WebFlux bridge usage](rabbit-webflux-bridge-usage.md)

## Rabbit RPC Bridge

Rabbit RPC is request/response. It is separate from event messaging. `AsyncRabbitTemplate` is RPC only. RPC does not use outbox by default.

Current status: planned, not implemented in this repository yet. There is no `reliable-message-rpc-rabbit-webflux-bridge` module and no `ReactiveRabbitRpcClient` implementation available today.

Planned direction:

```text
ReactiveRabbitRpcClient
AsyncRabbitTemplate
Mono.fromFuture
timeout/retry/circuit-breaker/bulkhead
```

Do not use:

```text
RabbitTemplate for RPC
AsyncRabbitTemplate for event publishing
outbox for normal RPC by default
Rabbit event retry queues as RPC retry semantics
```

Planned conceptual flow:

```text
WebFlux caller
 -> ReactiveRabbitRpcClient
 -> AsyncRabbitTemplate request/reply
 -> CompletableFuture
 -> Mono.fromFuture
 -> timeout/retry/circuit-breaker/bulkhead
 -> response or error
```

If a command must be durable, model it as an async command/event workflow instead of normal RPC.

## Optional RPC Context Propagation

MVC HTTP clients can use `RpcContextHolder`:

```java
RpcContextHolder.set(RpcContext.builder()
    .correlationId(correlationId)
    .requestId(requestId)
    .traceId(traceId)
    .build());
```

WebFlux HTTP clients use Reactor Context:

```java
return webClient.get()
    .uri("/customers/{id}", customerId)
    .retrieve()
    .bodyToMono(CustomerResponse.class)
    .contextWrite(ReactiveRpcContext.write(RpcContext.builder()
        .correlationId(correlationId)
        .requestId(requestId)
        .traceId(traceId)
        .build()));
```

RPC support is for propagation, timeout/retry conventions, resilience and metrics. It does not replace event messaging APIs.

## Audit Extension

Audit logging is opt-in compliance capture. It is not normal observability logging.

Use audit when you need:

```text
full payload or header capture
regulatory evidence
tamper-evident hashes/signatures
custom compliance sink
optional durable audit buffer
```

Default behavior stays safe:

```text
audit disabled
no full payload
no full headers
metadata-only observability
```

Example config:

```yaml
message:
  reliability:
    audit:
      enabled: true
      include-headers: true
      include-payload: false
      include-raw-body: false
      hash-enabled: true
      on-failure: continue-and-log
```

Register a `MessageAuditSink` or `ReactiveMessageAuditSink` to send audit records to your audit store, SIEM, or compliance pipeline.

Conceptual flow:

```text
publish/consume boundary
 -> observation
 -> audit capture policy
 -> sanitizer
 -> hash/signature
 -> audit sink
 -> optional durable audit buffer
```

WebFlux audit sinks must stay reactive unless explicitly isolated behind a documented blocking bridge boundary.

## Adapt An Existing System

Start with consumers before changing publishers:

1. Add idempotency to existing consumers.
2. Keep existing business handlers and wrap them with `@ReliableListener` or `@ReactiveReliableListener`.
3. Add outbox publishing for new writes that must publish reliably after database changes.
4. Enable retry and DLQ/DLT conventions.
5. Add RPC propagation and audit only where needed.

For an existing MVC service using JDBC, keep JDBC and use MVC modules. For a WebFlux service, use R2DBC or Reactive Redis providers and avoid JDBC in reactive flows.

## Common Anti-Patterns

Avoid these patterns:

- Using `AsyncRabbitTemplate` for event publishing.
- Using `RabbitTemplate` directly inside a WebFlux event-loop thread.
- Claiming the Rabbit WebFlux bridge is fully reactive RabbitMQ, native Reactor RabbitMQ, or non-blocking broker I/O.
- Adding outbox to normal RPC by default.
- Using JDBC inside a WebFlux reactive flow.
- Acking a Rabbit message before the handler `Mono` completes successfully.
- Acking before idempotency `markSuccess` succeeds.
- Using unbounded `flatMap` or unbounded queues.
- Treating virtual threads as unlimited concurrency.
- Mixing `ReliablePublisher` and `ReliableRpcClient` behind a single generic transport API.
- Treating observability logs as audit records.

## Operational Checks

For production readiness, verify:

- Outbox backlog is visible when the outbox module and flush scheduler are configured.
- Duplicate outcomes are visible.
- Retry and DLQ/DLT outcomes are visible for event messaging.
- Bridge executor active, queued and rejected metrics are visible for Rabbit WebFlux bridge.
- RPC timeout, retry, circuit breaker and bulkhead outcomes are visible for RPC clients.
- Audit failures follow the configured failure policy.
- Full audit capture has retention, access control and security policy.
