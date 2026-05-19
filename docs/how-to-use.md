# How To Use Reliable Message

This guide shows the shortest path to adopt `reliable-message` in an existing Spring Boot service.

## 1. Pick The Runtime Stack

Use the stack that matches your application:

| Existing app | Recommended modules |
| --- | --- |
| Spring MVC + RabbitMQ | `reliable-message-mvc-starter` |
| Spring MVC + Kafka | `reliable-message-mvc-starter`, `reliable-message-kafka-mvc` |
| Spring WebFlux + Kafka | `reliable-message-webflux-starter`, `reliable-message-kafka-webflux` |
| WebFlux storage | `reliable-message-outbox-r2dbc`, `reliable-message-idempotency-r2dbc`, or `reliable-message-idempotency-redis-reactive` |
| RPC calls | `reliable-message-rpc-mvc` or `reliable-message-rpc-webflux` |
| Compliance audit | `reliable-message-audit-mvc` or `reliable-message-audit-webflux` |

RabbitMQ is stable for MVC. WebFlux services should use Kafka today; WebFlux RabbitMQ is not shipped as production support.

## 2. Add Dependencies

MVC RabbitMQ:

```xml
<dependency>
  <groupId>io.github.huynhngochuyhoang</groupId>
  <artifactId>reliable-message-mvc-starter</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
```

WebFlux Kafka:

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
```

## 3. Configure Messaging

MVC RabbitMQ example:

```yaml
message:
  reliability:
    runtime: mvc
    transport: rabbit
    service-name: order-service
    rabbit:
      exchange: app.events
      queue-prefix: order-service.
    outbox:
      enabled: true
    idempotency:
      enabled: true
```

WebFlux Kafka example:

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
```

## 4. Publish Events

MVC:

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

WebFlux:

```java
return reactivePublisher.publish(
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

## 5. Consume Events

MVC:

```java
@ReliableListener("order.created")
public void handle(ReliableMessage<OrderCreatedEvent> message) {
    orderService.handle(message.payload());
}
```

WebFlux:

```java
@ReactiveReliableListener("order.created")
public Mono<Void> handle(ReliableMessage<OrderCreatedEvent> message) {
    return orderService.handle(message.payload());
}
```

WebFlux listener methods must return `Mono<Void>`.

## 6. Adapt An Existing System

Start with consumers before changing publishers:

1. Add idempotency to existing consumers.
2. Keep existing business handlers and wrap them with `@ReliableListener` or `@ReactiveReliableListener`.
3. Add outbox publishing for new writes that must publish reliably after database changes.
4. Enable retry and DLT conventions.
5. Add RPC propagation and audit only where needed.

For an existing MVC service using JDBC, keep JDBC and use the MVC modules. For a WebFlux service, use R2DBC or Reactive Redis providers and avoid JDBC in reactive flows.

## 7. Optional RPC Propagation

MVC HTTP clients use `RpcContextHolder`:

```java
RpcContextHolder.set(RpcContext.builder()
    .correlationId(correlationId)
    .requestId(requestId)
    .traceId(traceId)
    .build());
```

WebFlux HTTP clients use Reactor Context:

```java
return webClient.get()
    .uri("/customers/{id}", customerId)
    .retrieve()
    .bodyToMono(CustomerResponse.class)
    .contextWrite(ReactiveRpcContext.write(RpcContext.builder()
        .correlationId(correlationId)
        .requestId(requestId)
        .traceId(traceId)
        .build()));
```

RPC support is for propagation, timeout/retry conventions, and metrics. It does not replace messaging APIs.

## 8. Optional Audit

Audit is disabled by default and captures no full payload or full headers unless explicitly enabled.

```yaml
message:
  reliability:
    audit:
      enabled: true
      include-headers: true
      include-payload: false
      include-raw-body: false
      hash-enabled: true
      on-failure: continue-and-log
```

Register your own `MessageAuditSink` or `ReactiveMessageAuditSink` to send records to your audit store, SIEM, or compliance pipeline.

## 9. Operational Notes

- Use outbox for publish-after-database-write reliability.
- Use idempotency keys for effectively-once consumers.
- Use retry/DLT for recoverable processing failures.
- Keep RabbitMQ and Kafka semantics separate.
- Keep MVC blocking infrastructure out of WebFlux pipelines.
- Do not enable full audit capture without a retention and security policy.
