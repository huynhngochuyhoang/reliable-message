# Reliable Message Spring Boot Design

Opinionated reliability and observability framework for message-driven Spring Boot systems.

The architecture is split into two runtime stacks:

```text
Stack A: Spring MVC / blocking
Stack B: Spring WebFlux / reactive and hybrid bridge support
```

The framework provides effectively-once processing through outbox, idempotency, retry and observability conventions. It does not guarantee true exactly-once delivery.

## Current Status

The implemented direction keeps the original MVC/WebFlux split and transport separation:

```text
MVC + RabbitMQ/Kafka: blocking adapters
WebFlux + Kafka: reactive adapter
WebFlux + RabbitMQ: blocking bridge, hybrid mode, migration support
Rabbit RPC: separate request/response extension
```

RabbitMQ WebFlux support is not positioned as native Reactor RabbitMQ. It is a blocking Spring AMQP bridge isolated behind explicit executors, bounded concurrency and event-loop safety checks.

## 1. Architecture Principles

### Runtime Separation

MVC modules may use blocking infrastructure:

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

WebFlux modules should use reactive infrastructure unless a module is explicitly documented as a blocking bridge:

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

RabbitMQ WebFlux bridge is the explicit exception:

```text
Spring WebFlux API boundary
Spring AMQP blocking Rabbit layer
Dedicated bridge executor/scheduler
Bounded concurrency guard
Fail-fast overload behavior
```

### Transport Separation

RabbitMQ and Kafka have different delivery, retry and ordering semantics. The framework exposes common concepts, but transport adapters must keep transport-specific behavior visible.

```text
RabbitMQ: exchange, routing key, queue, retry queue, DLQ, ack/nack
Kafka: topic, partition key, consumer group, retry topic, DLT, offset commit
```

### Event Messaging vs RPC

Event messaging and RPC are separate communication models.

Event messaging:

```text
publish/consume
eventual consistency
outbox
idempotent consumer
retry queues or retry topics
DLQ/DLT
```

RPC:

```text
request/response
latency sensitive
timeout/retry/circuit breaker/bulkhead
caller-visible failure
no outbox by default
```

Hard rule:

```text
ReliablePublisher != ReliableRpcClient
ReactiveReliablePublisher != ReactiveRabbitRpcClient
```

Rabbit-specific rule:

```text
RabbitTemplate is for event messaging only.
AsyncRabbitTemplate is for Rabbit RPC only.
```

## 2. Shared Core

Module:

```text
reliable-message-core
```

Shared concepts are runtime-neutral and transport-neutral:

```text
ReliableMessage
PublishOptions
message headers
serializer abstraction
retry metadata
message status
dead-letter model
error model
```

Example envelope:

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

Example publish options:

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

RabbitMQ may ignore `partitionKey`. Kafka should use `partitionKey` as the record key.

## 3. Compatibility Matrix

| Runtime Stack | App Runtime | Storage | Idempotency | RabbitMQ | Kafka |
|---|---|---|---|---|---|
| MVC | Spring MVC | JDBC / JPA | JDBC / Redis | Supported | Supported |
| WebFlux | Spring WebFlux | R2DBC | R2DBC / Reactive Redis | Blocking bridge / hybrid mode | Supported |

Recommended production combinations:

```text
MVC + RabbitMQ + JDBC + Redis
MVC + Kafka + JDBC + Redis
WebFlux + Kafka + R2DBC + Reactive Redis
WebFlux + RabbitMQ blocking bridge for migration support
```

Avoid by default:

```text
WebFlux + JDBC inside reactive flow
WebFlux + blocking Redis inside reactive flow
Blocking RabbitMQ work on Netty event-loop threads
```

## 4. MVC Blocking Stack

Main starter:

```text
reliable-message-mvc-starter
```

Modules:

```text
reliable-message-outbox-jdbc
reliable-message-idempotency-jdbc
reliable-message-idempotency-redis
reliable-message-rabbit-mvc
reliable-message-kafka-mvc
```

Programming model:

```java
public interface ReliablePublisher {
    void publish(String eventName, Object payload, PublishOptions options);
}
```

```java
@ReliableListener("order.created")
public void handle(ReliableMessage<OrderCreatedEvent> message) {
    orderService.handle(message.payload());
}
```

MVC outbox flow:

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

MVC idempotent consumer flow:

```text
receive message
 -> extract idempotencyKey
 -> tryStart
 -> duplicate SUCCESS? ack and skip
 -> execute business handler
 -> markSuccess
 -> ack/commit
 -> on error: markFailed and retry/DLQ or DLT
```

Rabbit MVC uses `RabbitTemplate`, listener containers, publisher confirms, topology declaration and Rabbit-native retry/DLQ conventions.

Kafka MVC uses Kafka producers/consumers, retry topics, DLT and manual offset commit after success.

## 5. WebFlux Reactive Stack

Main starter:

```text
reliable-message-webflux-starter
```

Reactive modules:

```text
reliable-message-outbox-r2dbc
reliable-message-idempotency-r2dbc
reliable-message-idempotency-redis-reactive
reliable-message-kafka-webflux
```

Programming model:

```java
public interface ReactiveReliablePublisher {
    Mono<Void> publish(String eventName, Object payload, PublishOptions options);
}
```

```java
@ReactiveReliableListener("order.created")
public Mono<Void> handle(ReliableMessage<OrderCreatedEvent> message) {
    return orderService.handle(message.payload());
}
```

Reactive outbox flow:

```text
WebFlux request
 -> TransactionalOperator
 -> save business data using R2DBC
 -> save outbox row using R2DBC
 -> commit reactive transaction
 -> reactive outbox publisher flushes later
 -> publish to Kafka or supported event adapter
 -> mark outbox row as published
```

Reactive rules:

```text
do not call JDBC inside reactive outbox
do not block Netty event loop
do not use block() inside framework reactive code
preserve Reactor Context
use Reactive Redis or R2DBC for reactive idempotency
```

## 6. WebFlux Kafka Adapter

Module:

```text
reliable-message-kafka-webflux
```

Responsibilities:

```text
ReactiveKafkaReliablePublisher
ReactiveKafkaReliableListenerContainer
backpressure-aware consumption
bounded prefetch and concurrency
retry topic support
DLT support
offset commit after handler Mono completion
Reactor Context propagation
```

Consume flow:

```text
receive Kafka record
 -> deserialize ReliableMessage
 -> idempotency tryStart
 -> duplicate SUCCESS? commit offset
 -> invoke reactive handler
 -> markSuccess
 -> commit offset
 -> on error: retry topic or DLT
```

## 7. WebFlux Rabbit Event Bridge

Module:

```text
reliable-message-rabbit-webflux-bridge
```

Positioning:

```text
blocking bridge
hybrid mode
migration support
virtual-thread optimized blocking support
```

This module uses Spring AMQP and RabbitTemplate for event messaging. It is not native Reactor RabbitMQ and it is not non-blocking RabbitMQ broker I/O.

Implemented direction:

```text
ReactiveRabbitBridgePublisher
RabbitBridgeExecutorProvider
PlatformThreadRabbitBridgeExecutorProvider
VirtualThreadRabbitBridgeExecutorProvider
RabbitBridgeConcurrencyGuard
ReactiveRabbitBridgeListenerRegistrar
ReactiveRabbitBridgeMessageHandler
RabbitBridgeMetrics
```

Event publish flow:

```text
ReactiveReliablePublisher.publish
 -> serialize ReliableMessage
 -> acquire concurrency permit
 -> submit blocking RabbitTemplate.convertAndSend to bridge executor
 -> release permit on success, failure, cancellation or rejection
 -> return Mono<Void>
```

Event consume flow, Strategy A:

```text
Spring AMQP listener thread
 -> deserialize ReliableMessage
 -> ReactiveIdempotencyStore.tryStart
 -> duplicate SUCCESS? ack without handler
 -> duplicate PROCESSING/FAILED? fail/nack through failure path
 -> invoke @ReactiveReliableListener Mono<Void>
 -> wait for Mono completion at the bridge boundary
 -> markSuccess
 -> ack only after markSuccess succeeds
 -> on error: markFailed and nack/failure hook
```

Bridge safety rules:

```text
RabbitTemplate is event messaging only.
AsyncRabbitTemplate must not be used in this event bridge.
Blocking Rabbit operations must never run on Netty event-loop threads.
Bridge overload must remain bounded and deterministic.
First version supports fail-fast rejection only.
No outbox integration is included in Milestone 14 unless requested later.
```

Virtual thread semantics:

```text
virtual threads reduce blocking cost
virtual threads are not reactive
virtual threads still require concurrency limits
virtual threads do not remove backpressure concerns
```

## 8. Rabbit RPC Extension

Rabbit RPC is separate from Rabbit event messaging.

Implemented WebFlux Rabbit RPC module:

```text
reliable-message-rpc-rabbit-webflux-bridge
```

Main API:

```text
ReactiveRabbitRpcClient
```

Transport implementation:

```text
AsyncRabbitTemplate request/reply
dedicated RPC bridge executor offload
CompletableFuture to Mono boundary
caller-visible timeout
bounded RPC retry and fail-fast bulkhead
raw or envelope responses
ParameterizedTypeReference<T> responses
platform or virtual-thread executor mode
RPC metrics
```

RPC does not use outbox by default. Rabbit RPC circuit-breaker integration is not implemented. If a command must be durable, model it as an async command/event workflow instead of normal RPC.

## 9. Observability

Shared observability module:

```text
reliable-message-observability
```

Common event metrics:

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

Common tags:

```text
runtime=mvc|webflux|webflux-bridge
transport=rabbit|kafka
event_name=order.created
consumer=order-service
status=success|failed|duplicate|retry|dlq
```

Rabbit WebFlux bridge metrics include:

```text
message_rabbit_bridge_publish_total
message_rabbit_bridge_consume_total
message_rabbit_bridge_duplicate_total
message_rabbit_bridge_failure_outcome_total
message_rabbit_bridge_executor_rejected_total
message_rabbit_bridge_executor_active
message_rabbit_bridge_executor_queued
```

Bridge-specific tags:

```text
runtime=webflux-bridge
transport=rabbit
executor_mode=platform|virtual-thread
event_name=order.created
status=success|failure|duplicate|retry|dlq|rejected
```

Trace continuity target:

```text
HTTP request
 -> outbox save when configured
 -> publish event
 -> consume event
 -> business handler
 -> downstream RPC if needed
```

## 10. Admin API

Module:

```text
reliable-message-admin-api
```

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

Admin APIs must be disabled by default or protected by internal security configuration.

## 11. Roadmap

Current roadmap direction:

```text
1. Shared core
2. MVC Rabbit MVP
3. MVC idempotency
4. MVC outbox
5. MVC Rabbit retry and DLQ
6. Observability and admin API
7. MVC Kafka
8. WebFlux core
9. WebFlux storage
10. WebFlux Kafka
11. WebFlux hardening
12. RPC extension
13. Audit extension
14. Rabbit WebFlux blocking bridge and separate Rabbit RPC bridge
```

Milestone 14 event bridge order:

```text
14.1 module scaffold and properties
14.2 bridge executor and scheduler
14.3 concurrency guard and fail-fast rejection
14.4 ReactiveRabbitBridgePublisher event publish
14.5 Strategy A listener bridge
14.6 idempotency, duplicate and failure semantics
14.7 event-loop protection and bounded overload behavior
14.8 bridge observability
14.8.1 reactive R2DBC outbox flusher
14.8.2 dialect-aware R2DBC outbox schema configuration
14.8.3 generic and PostgreSQL optimized outbox claim strategies
14.9 separate Rabbit RPC bridge using AsyncRabbitTemplate
14.10 Rabbit RPC request/response
14.10.1 Rabbit RPC typing, envelope and executor hardening
14.11 bounded Rabbit RPC retry, bulkhead and metrics
14.12 user-facing documentation and limitations
```

## 12. Design Rules

Runtime rules:

```text
MVC starter uses blocking infrastructure.
WebFlux starter uses reactive infrastructure.
Rabbit WebFlux bridge is an explicit blocking bridge.
Do not run blocking Rabbit work on Netty event-loop threads.
Do not hide blocking infrastructure behind misleading reactive wording.
```

Reliability rules:

```text
Use effectively-once language, not exactly-once.
Ack or commit only after business processing succeeds.
Use idempotency before executing business logic.
Treat duplicate messages as normal.
Make poison messages visible.
Make retry count visible.
Make outbox backlog visible.
```

Transport rules:

```text
RabbitTemplate belongs to event messaging.
AsyncRabbitTemplate belongs to Rabbit RPC.
Outbox belongs to event messaging, not normal RPC.
Rabbit retry/DLQ is not RPC retry or bulkhead behavior.
Rabbit RPC circuit-breaker integration is not implemented.
```

Final recommendation:

```text
Use MVC + RabbitMQ or MVC + Kafka for blocking services.
Use WebFlux + Kafka for fully reactive messaging.
Use WebFlux + RabbitMQ blocking bridge for hybrid mode and migration support.
Keep Rabbit RPC separate from event messaging.
```
