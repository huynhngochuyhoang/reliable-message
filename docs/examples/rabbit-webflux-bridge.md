# WebFlux + RabbitMQ Blocking Bridge Example

Use this when a WebFlux service must use RabbitMQ through Spring AMQP.

This is blocking bridge / hybrid mode / migration support. It is not fully reactive RabbitMQ and not non-blocking broker I/O.

## Module

```xml
<dependency>
  <groupId>io.github.huynhngochuyhoang</groupId>
  <artifactId>reliable-message-rabbit-webflux-bridge</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
```

This event bridge uses `RabbitTemplate` internally. `AsyncRabbitTemplate` is RPC only.

## Platform Executor Configuration

```yaml
message:
  reliability:
    runtime: webflux
    transport: rabbit
    mode: blocking-bridge
    service-name: order-service
    rabbit:
      exchange: app.events
      auto-declare: true
      listener-auto-startup: true
      bridge:
        enabled: true
        executor-mode: platform
        worker-threads: 16
        queue-capacity: 1000
        max-concurrency: 256
        rejection-policy: fail-fast
```

## Virtual-Thread Executor Configuration

```yaml
message:
  reliability:
    runtime: webflux
    transport: rabbit
    mode: blocking-bridge
    service-name: order-service
    rabbit:
      exchange: app.events
      bridge:
        enabled: true
        executor-mode: virtual-thread
        queue-capacity: 1000
        max-concurrency: 1000
        rejection-policy: fail-fast
```

Virtual threads reduce blocking cost. They are not reactive and do not remove concurrency limits.

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
                .idempotencyKey(event.eventId())
                .correlationId(event.correlationId())
                .build()
        );
    }
}
```

Publish flow:

```text
ReactiveReliablePublisher.publish
 -> serialize ReliableMessage
 -> event-loop safety check
 -> acquire concurrency permit
 -> bridge executor platform/virtual-thread
 -> RabbitTemplate.convertAndSend
 -> release permit
 -> Mono completes or errors
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

Current listener strategy is Strategy A:

```text
Spring AMQP listener thread
 -> deserialize ReliableMessage
 -> ReactiveIdempotencyStore.tryStart
 -> invoke handler Mono
 -> wait at bridge boundary
 -> markSuccess
 -> ack after success
```

Ack happens only after the handler `Mono` and idempotency `markSuccess` complete successfully. Strategy B async ack coordination is not implemented.

## Fail-Fast Rejection

When bridge capacity is exhausted, publish fails with:

```text
RabbitBridgeRejectedException
```

The first version supports `fail-fast` only. It does not implement `block-caller` or `drop-and-metric` rejection policies.

## Event-Loop Safety

If publish is called from a Reactor/Netty event-loop-style thread, the bridge emits a warning or safety signal. The blocking Rabbit work is still offloaded to the bridge executor.

Do not call `RabbitTemplate` directly from WebFlux handlers.

## Metrics Tags

Bridge metrics include these tags:

```text
runtime=webflux-bridge
transport=rabbit
executor_mode=platform|virtual-thread
event_name=<event name>
status=<status>
```

Important metrics:

```text
message_rabbit_bridge_publish_total
message_rabbit_bridge_consume_total
message_rabbit_bridge_duplicate_total
message_rabbit_bridge_failure_outcome_total
message_rabbit_bridge_executor_rejected_total
message_rabbit_bridge_executor_active
message_rabbit_bridge_executor_queued
```

## Do Not Do This

- Do not use `AsyncRabbitTemplate` for event publishing.
- Do not call `RabbitTemplate` directly in WebFlux handlers.
- Do not ack before the handler `Mono` completes.
- Do not treat virtual threads as unlimited concurrency.
- Do not describe this module as fully reactive RabbitMQ.
