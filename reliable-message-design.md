# Reliable Message Spring Boot Design

Opinionated reliability + observability framework for message-driven Spring Boot systems.

This design is split into **two independent runtime stacks**:

```text
Stack A: Spring MVC / Blocking
Stack B: Spring WebFlux / Reactive
```

Do not mix blocking infrastructure into the WebFlux stack.

---

# 0. Positioning

Recommended positioning:

```text
Opinionated message reliability framework for Spring Boot.
```

Avoid positioning as:

```text
Exactly-once messaging framework.
```

This framework does not guarantee true exactly-once delivery.

It provides:

```text
effectively-once processing
outbox pattern
idempotent consumer
retry convention
dead-letter handling
observability
tracing
metrics
admin tooling
```

---

# 1. Stack Split

## Stack A - Spring MVC / Blocking

Use this stack for traditional blocking Spring Boot services.

```text
Spring MVC
JDBC / JPA
Spring Transaction Management
Spring AMQP / RabbitTemplate
Spring Kafka
Blocking Redis
Micrometer
OpenTelemetry
```

Main starter:

```text
reliable-message-mvc-starter
```

Supported transports:

```text
RabbitMQ
Kafka
```

Storage:

```text
JDBC outbox
JDBC idempotency
Redis idempotency
```

Programming model:

```text
ReliablePublisher
@ReliableListener
```

---

## Stack B - Spring WebFlux / Reactive

Use this stack for fully reactive Spring Boot services.

```text
Spring WebFlux
R2DBC
TransactionalOperator
ReactiveRedisTemplate
Reactor Kafka
Reactor Context
Micrometer
OpenTelemetry
```

Main starter:

```text
reliable-message-webflux-starter
```

Supported transports:

```text
Kafka first
RabbitMQ later / experimental
```

Storage:

```text
R2DBC outbox
R2DBC idempotency
Reactive Redis idempotency
```

Programming model:

```text
ReactiveReliablePublisher
@ReactiveReliableListener
```

Hard rule:

```text
No JDBC inside WebFlux starter.
No blocking Redis inside WebFlux starter.
No block() inside framework reactive code.
```

---

# 2. Runtime Compatibility Matrix

| Runtime Stack | App Runtime | DB Layer | Idempotency | RabbitMQ | Kafka |
|---|---|---|---|---|---|
| MVC | Spring MVC | JDBC / JPA | JDBC / Redis | Supported | Supported |
| WebFlux | Spring WebFlux | R2DBC | R2DBC / Reactive Redis | Later / experimental | Supported |

Recommended initial production combinations:

```text
MVC + RabbitMQ + JDBC + Redis
MVC + Kafka + JDBC + Redis
WebFlux + Kafka + R2DBC + Reactive Redis
```

Avoid as default design:

```text
WebFlux + JDBC
WebFlux + blocking Redis
WebFlux + blocking Rabbit listener
```

---

# 3. Shared Core

Module:

```text
reliable-message-core
```

This module is shared by both stacks.

It contains only runtime-neutral concepts.

## ReliableMessage

```java
public record ReliableMessage<T>(
    String messageId,
    String eventName,
    String aggregateId,
    String idempotencyKey,
    String correlationId,
    String traceId,
    Instant occurredAt,
    Map<String, String> headers,
    T payload
) {}
```

## PublishOptions

```java
public record PublishOptions(
    String aggregateId,
    String idempotencyKey,
    String correlationId,
    String partitionKey,
    Map<String, String> headers
) {}
```

Meaning:

```text
aggregateId = business aggregate identity
idempotencyKey = duplicate protection key
correlationId = request/message correlation
partitionKey = Kafka ordering key
headers = custom metadata
```

RabbitMQ may ignore `partitionKey`.

Kafka should use `partitionKey` as the Kafka record key.

## Shared Core Responsibilities

```text
message envelope
publish options
common headers
serializer abstraction
retry metadata
error model
message status
dead-letter record model
```

The core module should not depend on:

```text
Spring MVC
Spring WebFlux
JDBC
R2DBC
RabbitMQ
Kafka
Redis
```

---

# 4. Stack A - MVC / Blocking Design

## 4.1 MVC Modules

```text
reliable-message-mvc-starter
reliable-message-outbox-jdbc
reliable-message-idempotency-jdbc
reliable-message-idempotency-redis
reliable-message-rabbit-mvc
reliable-message-kafka-mvc
```

## 4.2 MVC Publisher API

```java
public interface ReliablePublisher {

    void publish(
        String eventName,
        Object payload,
        PublishOptions options
    );
}
```

Usage:

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

## 4.3 MVC Listener API

```java
@ReliableListener("order.created")
public void handle(
    ReliableMessage<OrderCreatedEvent> message
) {
    orderService.handle(message.payload());
}
```

Initial allowed return type:

```text
void
```

Do not support too many listener method shapes in the first version.

## 4.4 MVC Outbox

Module:

```text
reliable-message-outbox-jdbc
```

Table:

```sql
create table message_outbox (
    id varchar(64) primary key,
    event_name varchar(255) not null,
    aggregate_id varchar(255),
    idempotency_key varchar(255),
    partition_key varchar(255),
    payload jsonb not null,
    headers jsonb,
    status varchar(32) not null,
    retry_count int not null default 0,
    next_retry_at timestamp,
    created_at timestamp not null,
    published_at timestamp,
    last_error text
);
```

Flow:

```text
HTTP request
 -> @Transactional service
 -> save business data using JDBC/JPA
 -> save outbox row using JDBC
 -> commit DB transaction
 -> outbox flush job
 -> publish to RabbitMQ/Kafka
 -> mark outbox row as published
```

Blocking interface:

```java
public interface OutboxStore {

    void save(OutboxMessage message);

    List<OutboxMessage> findPending(int limit);

    void markPublished(String id);

    void markFailed(
        String id,
        Throwable error,
        Instant nextRetryAt
    );
}
```

## 4.5 MVC Idempotency

Modules:

```text
reliable-message-idempotency-jdbc
reliable-message-idempotency-redis
```

Interface:

```java
public interface IdempotencyStore {

    StartResult tryStart(
        String key,
        Duration ttl
    );

    void markSuccess(String key);

    void markFailed(
        String key,
        Throwable error
    );
}
```

States:

```text
PROCESSING
SUCCESS
FAILED
EXPIRED
```

Consumer flow:

```text
receive message
 -> extract idempotencyKey
 -> tryStart
 -> duplicate? ack and skip
 -> execute business handler
 -> markSuccess
 -> ack
 -> on error: markFailed and retry/DLQ
```

## 4.6 MVC RabbitMQ Adapter

Module:

```text
reliable-message-rabbit-mvc
```

Responsibilities:

```text
RabbitReliablePublisher
RabbitReliableListenerContainer
RabbitTopologyAutoConfigurer
RabbitDlqService
RabbitRetryStrategy
publisher confirm support
```

Naming convention:

```text
exchange = app.events
routingKey = order.created
queue = {service}.order.created
dlq = {service}.order.created.dlq

retry queues:
{service}.order.created.retry.5s
{service}.order.created.retry.1m
{service}.order.created.retry.5m
```

Config:

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

    retry:
      attempts: 5
      backoff:
        - 5s
        - 30s
        - 1m
        - 5m

    idempotency:
      enabled: true
      store: jdbc

    outbox:
      enabled: true
      store: jdbc
```

Rabbit retry flow:

```text
main queue
 -> fail
 -> retry queue with TTL
 -> back to main queue
 -> exceed attempts
 -> DLQ
```

## 4.7 MVC Kafka Adapter

Module:

```text
reliable-message-kafka-mvc
```

Responsibilities:

```text
KafkaReliablePublisher
KafkaReliableListenerContainer
Kafka retry topics
Kafka DLT
manual offset commit after success
partition key support
```

Naming convention:

```text
topic = order.created
consumer group = order-service
dlt = order.created.order-service.dlt

retry topics:
order.created.order-service.retry.5s
order.created.order-service.retry.1m
```

Config:

```yaml
message:
  reliability:
    runtime: mvc
    transport: kafka
    service-name: order-service

    kafka:
      topic-prefix: app.
      consumer-group: order-service

    retry:
      attempts: 5
      backoff:
        - 5s
        - 30s
        - 1m
        - 5m

    idempotency:
      enabled: true
      store: jdbc

    outbox:
      enabled: true
      store: jdbc
```

---

# 5. Stack B - WebFlux / Reactive Design

## 5.1 WebFlux Modules

```text
reliable-message-webflux-starter
reliable-message-outbox-r2dbc
reliable-message-idempotency-r2dbc
reliable-message-idempotency-redis-reactive
reliable-message-kafka-webflux
reliable-message-rabbit-webflux
```

`reliable-message-rabbit-webflux` is future/experimental.

## 5.2 WebFlux Publisher API

```java
public interface ReactiveReliablePublisher {

    Mono<Void> publish(
        String eventName,
        Object payload,
        PublishOptions options
    );
}
```

Usage:

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

## 5.3 WebFlux Listener API

```java
@ReactiveReliableListener("order.created")
public Mono<Void> handle(
    ReliableMessage<OrderCreatedEvent> message
) {
    return orderService.handle(message.payload());
}
```

Initial allowed return type:

```text
Mono<Void>
```

Optional later:

```text
Mono<T>, where result is ignored
```

Do not support blocking `void` handlers in the WebFlux starter.

## 5.4 WebFlux Outbox

Module:

```text
reliable-message-outbox-r2dbc
```

Flow:

```text
WebFlux request
 -> TransactionalOperator
 -> save business data using R2DBC
 -> save outbox row using R2DBC
 -> commit reactive transaction
 -> reactive outbox publisher flushes later
 -> publish to Kafka
 -> mark outbox row as published
```

Reactive interface:

```java
public interface ReactiveOutboxStore {

    Mono<Void> save(OutboxMessage message);

    Flux<OutboxMessage> findPending(int limit);

    Mono<Void> markPublished(String id);

    Mono<Void> markFailed(
        String id,
        Throwable error,
        Instant nextRetryAt
    );
}
```

Rules:

```text
do not call JDBC inside reactive outbox
do not block Netty event loop
do not use block()
use TransactionalOperator for R2DBC transactions
```

## 5.5 WebFlux Idempotency

Modules:

```text
reliable-message-idempotency-r2dbc
reliable-message-idempotency-redis-reactive
```

Interface:

```java
public interface ReactiveIdempotencyStore {

    Mono<StartResult> tryStart(
        String key,
        Duration ttl
    );

    Mono<Void> markSuccess(String key);

    Mono<Void> markFailed(
        String key,
        Throwable error
    );
}
```

Consumer flow:

```text
receive message
 -> extract idempotencyKey
 -> reactive tryStart
 -> duplicate? commit and stop
 -> execute reactive handler
 -> markSuccess
 -> commit
 -> on error: markFailed and retry/DLT
```

Rules:

```text
commit offset only after Mono completion
ack only after Mono completion
propagate failures through Reactor error signals
preserve Reactor Context
do not block inside reactive chain
```

## 5.6 WebFlux Kafka Adapter

Module:

```text
reliable-message-kafka-webflux
```

Recommended stack:

```text
Spring WebFlux
Reactor Kafka
R2DBC outbox
Reactive Redis / R2DBC idempotency
```

Responsibilities:

```text
ReactiveKafkaReliablePublisher
ReactiveKafkaReliableListenerContainer
backpressure-aware consumption
retry topic support
DLT support
offset commit after Mono completion
Reactor Context propagation
```

Config:

```yaml
message:
  reliability:
    runtime: webflux
    transport: kafka
    service-name: order-service

    kafka:
      topic-prefix: app.
      consumer-group: order-service

    reactive:
      max-concurrency: 64
      prefetch: 256

    retry:
      attempts: 5
      backoff:
        - 5s
        - 30s
        - 1m
        - 5m

    idempotency:
      enabled: true
      store: redis-reactive

    outbox:
      enabled: true
      store: r2dbc
```

Reactive Kafka consume flow:

```text
receive Kafka record
 -> deserialize ReliableMessage
 -> idempotency tryStart
 -> duplicate? commit offset
 -> invoke reactive handler
 -> markSuccess
 -> commit offset
 -> on error: retry topic or DLT
```

Backpressure rules:

```text
respect downstream demand
avoid unbounded flatMap
make concurrency configurable
make prefetch configurable
never block inside Kafka receive pipeline
```

## 5.7 WebFlux RabbitMQ Adapter

Module:

```text
reliable-message-rabbit-webflux
```

Status:

```text
future / experimental
```

Reason:

```text
Spring AMQP ecosystem is primarily listener-container based and blocking-oriented.
```

Do not claim fully non-blocking RabbitMQ support unless implementation uses a truly non-blocking RabbitMQ client path.

Possible future positioning:

```text
reactive API boundary over RabbitMQ
not guaranteed fully non-blocking
```

Default recommendation:

```text
Use RabbitMQ with MVC stack.
Use Kafka with WebFlux stack.
```

---

# 6. Observability

Module:

```text
reliable-message-observability
```

Applies to both stacks.

## Metrics

```text
message_publish_total
message_publish_failed_total
message_consume_total
message_consume_failed_total
message_consume_duration
message_retry_total
message_dlq_total
message_duplicate_total
message_outbox_pending_total
message_outbox_publish_duration
message_idempotency_check_duration
```

Tags:

```text
runtime=mvc|webflux
transport=rabbit|kafka
event_name=order.created
consumer=order-service
status=success|failed|duplicate
```

## Trace Spans

```text
message.publish
message.consume
message.outbox.save
message.outbox.flush
message.idempotency.check
message.retry
message.dlq
```

## Context Propagation

MVC:

```text
MDC
ThreadLocal tracing context
HTTP headers
message headers
```

WebFlux:

```text
Reactor Context
OpenTelemetry context
message headers
MDC bridge where possible
```

Trace continuity target:

```text
HTTP request
 -> outbox save
 -> publish
 -> consume
 -> business handler
```

---

# 7. Admin API

Module:

```text
reliable-message-admin-api
```

MVC endpoints use Spring MVC controllers.

WebFlux endpoints use reactive handlers/controllers.

Logical endpoints:

```http
GET  /internal/messages/outbox
POST /internal/messages/outbox/{id}/retry

GET  /internal/messages/dlq
POST /internal/messages/dlq/{id}/retry
POST /internal/messages/dlq/{id}/discard

GET  /internal/messages/idempotency/{key}
DELETE /internal/messages/idempotency/{key}
```

Operational goals:

```text
inspect outbox backlog
retry failed outbox records
inspect DLQ/DLT
retry DLQ/DLT messages
discard poison messages
inspect idempotency state
clear idempotency keys when safe
```

Security rule:

```text
admin API must be disabled by default or protected by internal security config
```

---

# 8. Module Structure

## Shared Modules

```text
reliable-message-core
reliable-message-observability
reliable-message-admin-api
```

## MVC Stack Modules

```text
reliable-message-mvc-starter
reliable-message-outbox-jdbc
reliable-message-idempotency-jdbc
reliable-message-idempotency-redis
reliable-message-rabbit-mvc
reliable-message-kafka-mvc
```

## WebFlux Stack Modules

```text
reliable-message-webflux-starter
reliable-message-outbox-r2dbc
reliable-message-idempotency-r2dbc
reliable-message-idempotency-redis-reactive
reliable-message-kafka-webflux
reliable-message-rabbit-webflux
```

## Optional Later

```text
reliable-message-testkit
reliable-message-sample-mvc-rabbit
reliable-message-sample-mvc-kafka
reliable-message-sample-webflux-kafka
```

---

# 9. Development Roadmap

## Milestone 1 - Shared Core

```text
ReliableMessage
PublishOptions
common headers
serializer abstraction
retry metadata
error model
dead-letter model
```

## Milestone 2 - MVC Rabbit MVP

```text
MVC starter
Rabbit publisher
Rabbit listener
JSON serialization
basic metrics
correlation ID propagation
```

## Milestone 3 - MVC Idempotency

```text
JDBC idempotency
Redis idempotency
consumer wrapper
duplicate detection
ack-after-success behavior
```

## Milestone 4 - MVC Outbox

```text
JDBC outbox
outbox flush scheduler
Rabbit publisher confirm
mark published / failed
retry failed publish
```

## Milestone 5 - MVC Rabbit Retry and DLQ

```text
Rabbit retry queues
Rabbit DLQ convention
poison message handling
DLQ retry
DLQ discard
```

## Milestone 6 - Observability and Admin API

```text
Micrometer metrics
OpenTelemetry spans
MDC propagation
admin endpoints
dashboard-friendly metric tags
```

## Milestone 7 - MVC Kafka

```text
Kafka publisher
Kafka listener
Kafka DLT
Kafka retry topics
manual offset commit
partition key support
```

## Milestone 8 - WebFlux Core

```text
WebFlux starter
ReactiveReliablePublisher
ReactiveReliableListener
ReactiveIdempotencyStore
ReactiveOutboxStore
Reactor Context propagation
```

## Milestone 9 - WebFlux Storage

```text
R2DBC outbox
R2DBC idempotency
Reactive Redis idempotency
TransactionalOperator support
no blocking calls
```

## Milestone 10 - WebFlux Kafka

```text
Reactor Kafka publisher
Reactor Kafka consumer
backpressure config
commit after Mono completion
retry topics
DLT
```

## Milestone 11 - WebFlux Rabbit Research

```text
evaluate non-blocking RabbitMQ client options
decide whether to support Rabbit WebFlux
clearly mark limitations
do not fake full non-blocking behavior
```

---

# 10. Design Rules

## Runtime Rules

```text
MVC starter uses blocking infrastructure.
WebFlux starter uses reactive infrastructure.
Do not mix JDBC into WebFlux starter.
Do not call block() inside framework reactive code.
Do not run blocking Redis calls inside reactive chains.
```

## Transport Rules

```text
Do not over-abstract RabbitMQ and Kafka.
RabbitMQ and Kafka have different delivery and retry models.
Expose common concepts, not fake identical behavior.
```

## Reliability Rules

```text
Ack or commit only after business processing succeeds.
Use idempotency before executing business logic.
Treat duplicate messages as normal.
Make poison messages visible.
Make retry count visible.
Make outbox backlog visible.
```

## Reactive Rules

```text
Preserve Reactor Context.
Commit Kafka offset after Mono completion.
Respect backpressure.
Avoid unbounded flatMap.
Make concurrency configurable.
Use R2DBC for reactive database work.
Use Reactive Redis for reactive idempotency.
```

---

# 11. Final Recommendation

Build order:

```text
1. MVC + RabbitMQ + JDBC outbox + idempotency + observability
2. MVC + Kafka
3. WebFlux + Kafka + R2DBC + Reactive Redis
4. RabbitMQ WebFlux research only after the above is stable
```

This keeps the framework practical, honest, and production-ready.
