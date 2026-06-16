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

Add R2DBC outbox when the WebFlux service must flush durable event rows through this bridge:

```xml
<dependency>
  <groupId>org.springframework.boot</groupId>
  <artifactId>spring-boot-starter-data-r2dbc</artifactId>
</dependency>
<dependency>
  <groupId>org.postgresql</groupId>
  <artifactId>r2dbc-postgresql</artifactId>
</dependency>
<dependency>
  <groupId>io.github.huynhngochuyhoang</groupId>
  <artifactId>reliable-message-outbox-r2dbc</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
```

Outbox remains event-messaging only. Do not use outbox for normal RPC.

Add one reactive idempotency provider. This Redis example is auto-configured when Spring provides a reactive Redis template:

```xml
<dependency>
  <groupId>org.springframework.boot</groupId>
  <artifactId>spring-boot-starter-data-redis-reactive</artifactId>
</dependency>
<dependency>
  <groupId>io.github.huynhngochuyhoang</groupId>
  <artifactId>reliable-message-idempotency-redis-reactive</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
```

```yaml
spring:
  data:
    redis:
      host: localhost
      port: 6379
```

The Redis provider requires a `ReactiveStringRedisTemplate`; Spring Boot creates it from the reactive Redis starter and connection configuration. The Rabbit bridge listener currently uses a 24-hour idempotency TTL internally. It does not expose a Rabbit-bridge TTL property yet. `reliable-message-idempotency-r2dbc` is the reactive database alternative and requires the same `ConnectionFactory` infrastructure as the R2DBC outbox.

The R2DBC outbox requires a `ConnectionFactory`. Spring Boot creates it from `spring.r2dbc.*` when the R2DBC starter and compatible driver are present. The PostgreSQL dependency above is an example; use the driver for your database:

```yaml
spring:
  r2dbc:
    url: r2dbc:postgresql://localhost:5432/orders
    username: orders
    password: change-me
```

Provision the `message_outbox` table with a database migration before enabling flushing. Auto-configuration does not call `R2dbcOutboxStore.initializeSchema()`. Tests and simple local environments may invoke that method explicitly during startup.

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
    outbox:
      enabled: true
      flush-enabled: true
      schema:
        payload-storage: json
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
        max-concurrency: 1000
        rejection-policy: fail-fast
```

Virtual threads reduce blocking cost. They are not reactive and do not remove concurrency limits.

## R2DBC Outbox Schema Configuration

When `reliable-message-outbox-r2dbc` is present and outbox flushing is enabled, schema column types resolve in this order:

1. User explicit config.
2. Dialect recommended default.
3. Generic fallback.

Common modes:

```yaml
message:
  reliability:
    outbox:
      enabled: true
      schema:
        payload-storage: text   # text | json; binary is planned and fails fast today
```

PostgreSQL JSON mode resolves `payload` and `headers` to `jsonb`; MySQL text mode resolves payload to `longtext`; SQL Server text mode resolves payload to `nvarchar(max)`.

Use advanced overrides when needed:

```yaml
message:
  reliability:
    outbox:
      schema:
        payload-column-type: clob
        headers-column-type: clob
        payload-bytes-column-type: blob
        last-error-column-type: clob
```

`payload-storage: binary` is planned, not supported by the current runtime store. It fails fast until binary payload codec and `payload_bytes` read/write support are implemented. PostgreSQL JSON mode uses dialect-aware `json/jsonb` binding.

Claim strategy is dialect-aware: the non-PostgreSQL fallback uses select-ID plus conditional-update claiming with `LIMIT` pagination and is only suitable for databases that support that syntax. PostgreSQL uses atomic `FOR UPDATE SKIP LOCKED` plus `UPDATE ... RETURNING` without a window function. MySQL, Oracle, and SQL Server optimized claim strategies are not implemented yet. Oracle and SQL Server are not supported by the current `LIMIT`-based fallback.

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

## Multiple Queue Listeners

Each `@ReactiveReliableListener` method registers a separate listener endpoint. The bridge derives the queue name from `service-name` and the event name:

```text
queue = {service-name}.{eventName}
```

With `service-name: order-service`, these listeners consume from separate queues:

```text
order-service.order.created
order-service.payment.captured
order-service.inventory.reserved
```

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

Each method must accept one `ReliableMessage<T>` argument and return `Mono<Void>`. Each listener uses Strategy A, manual ack, and prefetch `1`; ack still happens only after that method's `Mono` and `markSuccess` complete successfully.

Topology auto-declare creates and binds each queue when `message.reliability.rabbit.auto-declare=true`. If your broker topology is pre-provisioned, disable auto-declare and create the queues and bindings outside the application.

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

Handler failures are nacked and pass through the minimal event failure hook. Retry and DLQ outcome metrics are emitted only when the configured hook returns a concrete outcome. Broker retry/DLQ routing is infrastructure-owned; the bridge does not create or own DLQ transport routing.

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
message_rabbit_bridge_executor_active  # platform mode only
message_rabbit_bridge_executor_queued  # platform mode only
```

## Do Not Do This

- Do not use `AsyncRabbitTemplate` for event publishing.
- Do not call `RabbitTemplate` directly in WebFlux handlers.
- Do not ack before the handler `Mono` completes.
- Do not treat virtual threads as unlimited concurrency.
- Do not describe this module as fully reactive RabbitMQ.
