# MVC Rabbit Documentation

This document describes the completed Milestone 01-06 surface for the first production target:

```text
Spring MVC + RabbitMQ + JDBC outbox + idempotent consumers + observability + internal admin APIs
```

## Module Selection

Use the MVC starter for the default blocking RabbitMQ stack:

```xml
<dependency>
    <groupId>io.github.huynhngochuyhoang</groupId>
    <artifactId>reliable-message-mvc-starter</artifactId>
    <version>0.1.0-SNAPSHOT</version>
</dependency>
```

The starter brings in:

- `reliable-message-core`
- `reliable-message-mvc-api`
- `reliable-message-rabbit-mvc`
- `reliable-message-kafka-mvc`
- `reliable-message-observability`
- `reliable-message-outbox-jdbc`
- `reliable-message-idempotency-jdbc`
- `reliable-message-admin-api`

The starter is intentionally opinionated for storage: JDBC is the outbox store and JDBC is the default idempotency store. RabbitMQ remains the default transport when `message.reliability.transport` is not set; set `message.reliability.transport=kafka` for the Kafka adapter.

## Idempotency Modules

Applications should normally use one idempotency provider.

Available providers:

- `reliable-message-idempotency-jdbc`: JDBC-backed `IdempotencyStore`. This is bundled by `reliable-message-mvc-starter` and is the default for the current MVC Rabbit target.
- `reliable-message-idempotency-redis`: Redis-backed `IdempotencyStore`. Use this when idempotency state should live in Redis instead of the application database.

Both modules implement the same `IdempotencyStore` API and both auto-configurations back off when another `IdempotencyStore` bean already exists. Do not add both provider modules unless you intentionally control the selected bean yourself.

If both JDBC and Redis providers are on the classpath and both infrastructure beans exist, define your own `IdempotencyStore` bean or exclude the provider auto-configuration you do not want. Otherwise, the selected provider depends on auto-configuration ordering and should not be treated as an application contract.

## Configuration

Minimal MVC Rabbit configuration:

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
```

Retry and idempotency configuration:

```yaml
message:
  reliability:
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

JDBC outbox configuration:

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

Admin APIs are disabled by default:

```yaml
message:
  reliability:
    admin:
      enabled: false
      default-limit: 50
      max-limit: 200
```

Only enable admin endpoints behind internal network controls and application security:

```yaml
message:
  reliability:
    admin:
      enabled: true
```

## Publishing

Inject `ReliablePublisher` for immediate publishing:

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

Inject `OutboxPublisher` when the message should be saved in the same database transaction as business data and published later by the flush scheduler:

```java
outboxPublisher.publishLater(
    "order.created",
    event,
    PublishOptions.builder()
        .aggregateId(orderId)
        .idempotencyKey(eventId)
        .correlationId(correlationId)
        .build()
);
```

## Consuming

Declare blocking MVC consumers with `@ReliableListener`:

```java
@ReliableListener("order.created")
public void handle(ReliableMessage<OrderCreatedEvent> message) {
    orderService.handle(message.payload());
}
```

The Rabbit listener integration:

- deserializes the message envelope
- propagates correlation context through headers and MDC
- checks idempotency before invoking the business handler when an `IdempotencyStore` is available
- acknowledges duplicate successful deliveries without invoking the handler again
- acknowledges only after successful handler execution
- routes failures through retry queues and then the DLQ after retry attempts are exhausted

## Retry And DLQ

Retry behavior is controlled by `message.reliability.retry.attempts` and `message.reliability.retry.backoff`.

On handler failure:

1. the retry count is incremented
2. the message is republished to a retry route for the configured backoff
3. exhausted messages are routed to the DLQ

The Rabbit DLQ service supports retrying a DLQ message back to the main event route and creating a discard record for intentionally discarded messages.

## Observability

The observability module emits Micrometer metrics and Micrometer observations with low-cardinality tags:

- `runtime`
- `transport`
- `event_name`
- `consumer`
- `status`

Important metrics include:

- `message_publish_total`
- `message_publish_failed_total`
- `message_consume_total`
- `message_consume_failed_total`
- `message_consume_duration`
- `message_consume_failure_routed_total`
- `message_retry_total`
- `message_dlq_total`
- `message_dlq_operations_total`
- `message_duplicate_total`
- `message_outbox_pending_total`
- `message_outbox_publish_duration`
- `message_idempotency_check_duration`

## Admin API

When `message.reliability.admin.enabled=true`, the MVC admin module can expose internal endpoints for configured operations.

Outbox:

- `GET /internal/messages/outbox`
- `POST /internal/messages/outbox/{id}/retry`

DLQ:

- `GET /internal/messages/dlq`
- `POST /internal/messages/dlq/{id}/retry`
- `POST /internal/messages/dlq/{id}/discard`

Idempotency:

- `GET /internal/messages/idempotency/{key}`
- `DELETE /internal/messages/idempotency/{key}`

Some operations require operation beans from the runtime adapter or application. If an operation bean is absent, the endpoint may not be created or may return `501 Not Implemented`.

## Completed Milestones

- Milestone 01: runtime-neutral core models, headers, serializer contract, retry metadata, error model, status model, and dead-letter model.
- Milestone 02: MVC Rabbit publisher, listener registration, JSON serialization, basic metrics, and correlation propagation.
- Milestone 03: MVC idempotency API plus JDBC and Redis provider modules.
- Milestone 04: JDBC outbox store, transaction-friendly `OutboxPublisher`, and scheduled flush.
- Milestone 05: Rabbit retry topology, retry routing, DLQ routing, DLQ retry, and discard support.
- Milestone 06: observability module and disabled-by-default MVC admin API.
