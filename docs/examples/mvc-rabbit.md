# MVC + RabbitMQ Example

Use this for a blocking Spring MVC service that publishes and consumes RabbitMQ events.

## Modules

```xml
<dependency>
  <groupId>io.github.huynhngochuyhoang</groupId>
  <artifactId>reliable-message-mvc-starter</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
<dependency>
  <groupId>io.github.huynhngochuyhoang</groupId>
  <artifactId>reliable-message-rabbit-mvc</artifactId>
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
    outbox:
      initialize-schema: true
      flush-enabled: true
      batch-size: 100
      flush-delay: 5s
      retry-delay: 30s
```

`retry.attempts` and `retry.backoff` drive Rabbit retry routing and DLQ behavior. The outbox properties configure the JDBC outbox schema and flush scheduler.

## Publish An Event

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
                .idempotencyKey(event.eventId())
                .correlationId(event.correlationId())
                .build()
        );
    }
}
```

Use `OutboxPublisher` instead of immediate `ReliablePublisher` when the event must be saved in the same JDBC transaction as business data and published later by the flush scheduler.

## Consume An Event

```java
@Component
class OrderCreatedListener {

    @ReliableListener("order.created")
    public void handle(ReliableMessage<OrderCreatedEvent> message) {
        OrderCreatedEvent event = message.payload();
        // Update read model or trigger local business workflow.
    }
}
```

Consumer behavior:

```text
receive Rabbit message
 -> idempotency tryStart
 -> invoke handler
 -> markSuccess
 -> ack after success
 -> on failure: retry route, then DLQ after attempts are exhausted
```

## Do Not Do This

- Do not use `AsyncRabbitTemplate` for event publishing.
- Do not skip idempotency keys for events that may be redelivered.
- Do not publish from a database transaction without outbox when the event must reflect committed business data.
- Do not claim exactly-once delivery; use effectively-once wording.
