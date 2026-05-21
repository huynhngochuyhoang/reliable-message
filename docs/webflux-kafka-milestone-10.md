# WebFlux Kafka Documentation

This document describes the completed Milestone 10 Reactor Kafka adapter for WebFlux applications.

## Module

Add the WebFlux Kafka module:

```xml
<dependency>
    <groupId>io.github.huynhngochuyhoang</groupId>
    <artifactId>reliable-message-kafka-webflux</artifactId>
    <version>0.1.0-SNAPSHOT</version>
</dependency>
```

## Configuration

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
    retry:
      attempts: 5
      backoff:
        - 5s
        - 30s
        - 1m
        - 5m
```

## Publishing

`ReactiveKafkaReliablePublisher` implements `ReactiveReliablePublisher`:

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

The publisher serializes a `ReliableMessage`, writes standard reliable-message headers, and sends through Reactor Kafka without blocking.

## Consuming

Declare reactive listeners with `@ReactiveReliableListener`:

```java
@ReactiveReliableListener("order.created")
public Mono<Void> handle(ReliableMessage<OrderCreatedEvent> message) {
    return orderService.handle(message.payload());
}
```

The WebFlux Kafka handler commits the receiver offset only after the handler `Mono` completes.

## Retry And DLT

Failed records are republished to retry topics until attempts are exhausted:

```text
<topic>.<consumer-group>.retry.<delay>
```

After attempts are exhausted, failed records are routed to:

```text
<topic>.<consumer-group>.dlt
```

Retry metadata is stored in message headers:

- `x-retry-count`
- `x-retry-not-before`
- `x-original-message-id`
- `x-error-type`
- `x-error-message`

## Idempotency

If a `ReactiveIdempotencyStore` bean is available and the message has an idempotency key, the consumer flow is:

```text
tryStart
 -> duplicate: commit and stop
 -> handler Mono
 -> markSuccess
 -> commit offset
 -> on error: markFailed, route retry/DLT, commit, propagate error signal
```

## Completed Milestone

- `reliable-message-kafka-webflux` module
- Reactor Kafka publisher implementation
- Reactor Kafka receiver container and listener registrar
- Configurable max concurrency and prefetch
- Retry topic and DLT routing
- Offset commit after handler `Mono` completion
- Failure routing with Reactor error signal propagation
