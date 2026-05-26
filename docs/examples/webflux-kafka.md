# WebFlux + Kafka Example

Use this for a reactive WebFlux service that publishes and consumes Kafka events.

## Modules

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
<dependency>
  <groupId>io.github.huynhngochuyhoang</groupId>
  <artifactId>reliable-message-outbox-r2dbc</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
```

Add `reliable-message-idempotency-r2dbc` or `reliable-message-idempotency-redis-reactive` for reactive idempotency.

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
      listener-auto-startup: true
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
    idempotency:
      ttl: 24h
```

R2DBC outbox is provided by `R2dbcOutboxStore`. It uses `DatabaseClient` and can participate in caller-managed reactive transactions.

## Publish An Event

```java
@Service
class OrderApplicationService {

    private final ReactiveReliablePublisher publisher;

    OrderApplicationService(ReactiveReliablePublisher publisher) {
        this.publisher = publisher;
    }

    Mono<Void> orderCreated(String orderId, OrderCreatedEvent event) {
        return publisher.publish(
            "order.created",
            event,
            PublishOptions.builder()
                .aggregateId(orderId)
                .partitionKey(orderId)
                .idempotencyKey(event.eventId())
                .correlationId(event.correlationId())
                .build()
        );
    }
}
```

## Consume An Event

```java
@Component
class OrderCreatedListener {

    @ReactiveReliableListener("order.created")
    Mono<Void> handle(ReliableMessage<OrderCreatedEvent> message) {
        return orderProjection.update(message.payload());
    }
}
```

The listener method must return `Mono<Void>`.

Consumer behavior:

```text
receive Kafka record
 -> ReactiveIdempotencyStore.tryStart
 -> invoke handler Mono
 -> markSuccess after Mono success
 -> commit offset after success
 -> on error: markFailed, route retry/DLT, commit according to transport flow
```

## Do Not Do This

- Do not use JDBC inside a WebFlux reactive flow.
- Do not call blocking Redis from the reactive handler.
- Do not use unbounded `flatMap` or unbounded queues.
- Do not commit offsets before the handler `Mono` completes successfully.
