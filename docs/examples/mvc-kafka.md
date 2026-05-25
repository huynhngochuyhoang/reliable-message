# MVC + Kafka Example

Use this for a blocking Spring MVC service that publishes and consumes Kafka events.

## Modules

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
<dependency>
  <groupId>io.github.huynhngochuyhoang</groupId>
  <artifactId>reliable-message-outbox-jdbc</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
```

Add `reliable-message-idempotency-jdbc` or `reliable-message-idempotency-redis` when using those idempotency stores.

## Configuration

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
      partitions: 3
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
    outbox:
      initialize-schema: true
      flush-enabled: true
      batch-size: 100
      flush-delay: 5s
      retry-delay: 30s
```

Kafka retry topics and DLT are transport-specific. Keep them separate from Rabbit retry queue assumptions.

## Publish An Event With Partition Key

```java
@Service
class OrderApplicationService {

    private final ReliablePublisher publisher;

    OrderApplicationService(ReliablePublisher publisher) {
        this.publisher = publisher;
    }

    void orderCreated(String orderId, OrderCreatedEvent event) {
        publisher.publish(
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

Use `partitionKey` for ordering by aggregate. Kafka uses it as the record key.

## Consume An Event

```java
@Component
class OrderCreatedListener {

    @ReliableListener("order.created")
    public void handle(ReliableMessage<OrderCreatedEvent> message) {
        // Handler runs before the offset is committed.
        orderProjection.update(message.payload());
    }
}
```

Consumer behavior:

```text
receive Kafka record
 -> idempotency tryStart
 -> invoke handler
 -> markSuccess
 -> commit offset after success
 -> on failure: retry topic, then DLT after attempts are exhausted
```

## Do Not Do This

- Do not assume Rabbit queue/DLQ naming applies to Kafka topics.
- Do not omit `partitionKey` when aggregate ordering matters.
- Do not commit offsets before handler success.
- Do not claim exactly-once delivery; use effectively-once wording.
