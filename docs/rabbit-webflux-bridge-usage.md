# Rabbit WebFlux Blocking Bridge Usage

The Rabbit WebFlux bridge supports WebFlux services that must use RabbitMQ through Spring AMQP.

It is:

```text
blocking bridge
hybrid mode
migration support
virtual-thread optimized blocking support
```

It is not fully reactive RabbitMQ, not native Reactor RabbitMQ, and not non-blocking RabbitMQ broker I/O.

## Module

Add the event bridge module:

```xml
<dependency>
  <groupId>io.github.huynhngochuyhoang</groupId>
  <artifactId>reliable-message-rabbit-webflux-bridge</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
```

This module is for event messaging only. RabbitTemplate is blocking.

```text
RabbitTemplate = event messaging
AsyncRabbitTemplate = RPC only
```

## When To Use It

Use this bridge when:

- The application is WebFlux.
- RabbitMQ is required by an existing platform or migration plan.
- You accept that Spring AMQP is blocking infrastructure.
- Blocking Rabbit work can run on a dedicated bridge executor.
- Overload can fail fast instead of queueing without bounds.

Prefer WebFlux + Kafka for greenfield reactive messaging.

## Sample Configuration

Common configuration:

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
        max-concurrency: 256
        queue-capacity: 1000
        rejection-policy: fail-fast
```

`message.reliability.transport=rabbit` is the Rabbit transport selector. If the project default is Rabbit, the bridge may also match when the transport property is omitted.

## Platform Executor Mode

Use platform mode when you want a bounded platform thread pool:

```yaml
message:
  reliability:
    rabbit:
      bridge:
        executor-mode: platform
        worker-threads: 16
        queue-capacity: 1000
        max-concurrency: 256
        rejection-policy: fail-fast
```

Properties:

- `worker-threads`: platform worker count.
- `queue-capacity`: bounded executor queue capacity.
- `max-concurrency`: bridge operation concurrency limit.
- `rejection-policy`: first version supports `fail-fast` only.

## Virtual-Thread Executor Mode

Use virtual-thread mode on Java 21 when blocking Rabbit work is expected and you still keep concurrency bounded:

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

Virtual threads reduce blocking cost. They are not reactive and do not remove backpressure or capacity limits.

## Publish Events

Inject the WebFlux publisher API:

```java
@RequiredArgsConstructor
@Service
class OrderApplicationService {

    private final ReactiveReliablePublisher publisher;

    Mono<Void> publishOrderCreated(String orderId, OrderCreatedEvent event) {
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

Bridge publish flow:

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

`RabbitTemplate.convertAndSend` is blocking. It runs on the bridge executor, not inline on the caller thread.

## Consume Events

Use `@ReactiveReliableListener` with `Mono<Void>`:

```java
@Component
class OrderCreatedListener {

    @ReactiveReliableListener("order.created")
    Mono<Void> handle(ReliableMessage<OrderCreatedEvent> message) {
        return orderProjection.update(message.payload());
    }
}
```

## Multiple Queue Listeners

Register multiple event queues by declaring multiple `@ReactiveReliableListener` methods. The bridge creates one endpoint and listener container per annotated method. Queue names are derived from `service-name` and the event name:

```text
queue = {service-name}.{eventName}
```

For `service-name: order-service`:

```java
@Component
class OrderWorkflowListeners {

    @ReactiveReliableListener("order.created")
    Mono<Void> onOrderCreated(ReliableMessage<OrderCreatedEvent> message) {
        return orderProjection.create(message.payload());
    }

    @ReactiveReliableListener("payment.captured")
    Mono<Void> onPaymentCaptured(ReliableMessage<PaymentCapturedEvent> message) {
        return billingProjection.markPaid(message.payload());
    }

    @ReactiveReliableListener("inventory.reserved")
    Mono<Void> onInventoryReserved(ReliableMessage<InventoryReservedEvent> message) {
        return fulfillmentWorkflow.start(message.payload());
    }
}
```

The resulting queues are:

```text
order-service.order.created
order-service.payment.captured
order-service.inventory.reserved
```

Each listener container uses manual ack and prefetch `1`. Ack still happens only after that listener method's `Mono` and `markSuccess` complete successfully.

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

Ack rules:

- Duplicate `SUCCESS`: ack and skip the handler.
- Duplicate `PROCESSING` or `FAILED`: do not ack as success; use the failure path.
- New message: ack only after handler `Mono` completes and `markSuccess` succeeds.
- Handler failure: mark failed where possible, then nack/failure hook.

No Strategy B async ack coordination. Strategy B async ack coordination is not implemented.

## Fail-Fast Rejection

The bridge uses `RabbitBridgeConcurrencyGuard` before submitting blocking work.

When capacity is exhausted:

```text
RabbitBridgeRejectedException
```

is returned through the `Mono` error path. This is intentional. The first version avoids unbounded queueing and does not implement `block-caller` or `drop-and-metric` rejection policies.

## Event-Loop Warning

If publish is called from a Reactor/Netty event-loop-style thread, the bridge emits a warning or safety signal.

The warning does not change successful business behavior. It exists to surface unsafe usage. The actual blocking Rabbit work is still offloaded to the bridge executor.

Detected thread-name styles include:

```text
reactor-http-nio
reactor-http-epoll
reactor-http-kqueue
reactor-tcp-nio
nioEventLoop
epollEventLoop
kqueueEventLoop
```

## Metrics

Bridge metrics use these required tags:

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

Retry and DLQ outcome metrics are recorded only when event failure hooks expose concrete outcomes.

## Topology Declaration

The bridge can declare listener queues, exchange and bindings when topology auto-declaration is enabled. Disable auto-declare when broker topology is pre-provisioned or the service has read-only RabbitMQ permissions.

## Limitations

Keep these limits visible in service documentation:

- RabbitMQ work remains blocking Spring AMQP work.
- The bridge is not fully reactive RabbitMQ and not native Reactor RabbitMQ.
- Virtual threads are not unlimited concurrency.
- Backpressure is bounded through queue capacity, concurrency guard and fail-fast rejection.
- Listener Strategy A blocks at the bridge boundary until handler completion.
- Outbox integration is not part of Milestone 14 event bridge unless added later.
- Rabbit RPC belongs in a separate `AsyncRabbitTemplate`-based module.
