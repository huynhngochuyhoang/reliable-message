# MVC Kafka Documentation

This document describes the completed Milestone 07 Kafka adapter for the blocking MVC stack.

## Module Selection

Use the MVC starter and select Kafka as the active transport:

```xml
<dependency>
    <groupId>io.github.huynhngochuyhoang</groupId>
    <artifactId>reliable-message-mvc-starter</artifactId>
    <version>0.1.0-SNAPSHOT</version>
</dependency>
```

```yaml
message:
  reliability:
    runtime: mvc
    transport: kafka
```

The Kafka adapter can also be used directly:

```xml
<dependency>
    <groupId>io.github.huynhngochuyhoang</groupId>
    <artifactId>reliable-message-kafka-mvc</artifactId>
    <version>0.1.0-SNAPSHOT</version>
</dependency>
```

## Configuration

Minimal Kafka configuration:

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
      publish-timeout: 5s
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

## Publishing

`KafkaReliablePublisher` publishes the reliable message envelope to the configured topic prefix. `PublishOptions.partitionKey` is used as the Kafka record key.

```java
publisher.publish(
    "order.created",
    event,
    PublishOptions.builder()
        .aggregateId(orderId)
        .idempotencyKey(eventId)
        .correlationId(correlationId)
        .partitionKey(orderId)
        .build()
);
```

With `topic-prefix: app.`, this publishes to `app.order.created`.

## Consuming

Declare blocking MVC consumers with `@ReliableListener`:

```java
@ReliableListener("order.created")
public void handle(ReliableMessage<OrderCreatedEvent> message) {
    orderService.handle(message.payload());
}
```

The Kafka listener integration:

- creates a listener container for the prefixed event topic and configured retry topics
- uses the configured consumer group, or `service-name` when no group is configured
- uses manual offset acknowledgment
- checks idempotency before invoking the business handler when an `IdempotencyStore` is available
- commits duplicate successful deliveries without invoking the handler again
- commits only after successful handler execution, duplicate detection, or successful retry/DLT routing

## Retry And DLT

Kafka retry topics and DLT topics use the configured topic name and consumer group:

```text
app.order.created.order-service.retry.5s
app.order.created.order-service.retry.30s
app.order.created.order-service.retry.1m
app.order.created.order-service.retry.5m
app.order.created.order-service.dlt
```

On handler failure:

1. the retry count is incremented
2. the record is synchronously republished to the next retry topic
3. exhausted records are synchronously republished to the DLT
4. the original offset is committed only after routing succeeds

## Completed Milestone

- `reliable-message-kafka-mvc` module
- Kafka publisher implementation
- Kafka listener container integration
- retry topic and DLT naming
- retry and DLT routing
- manual offset commit after success
- Kafka record key support through `PublishOptions.partitionKey`
