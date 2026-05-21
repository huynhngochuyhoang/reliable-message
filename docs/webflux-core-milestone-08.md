# WebFlux Core Documentation

This document describes the completed Milestone 08 reactive API boundary for WebFlux applications.

## Starter

Add the WebFlux starter:

```xml
<dependency>
    <groupId>io.github.huynhngochuyhoang</groupId>
    <artifactId>reliable-message-webflux-starter</artifactId>
    <version>0.1.0-SNAPSHOT</version>
</dependency>
```

The starter provides reactive contracts and listener discovery only. It does not include JDBC, R2DBC, Redis, Kafka, or RabbitMQ implementations.

## Publisher Contract

```java
public interface ReactiveReliablePublisher {

    Mono<Void> publish(String eventName, Object payload, PublishOptions options);
}
```

Transport implementations are intentionally left for later milestones.

## Listener Contract

Declare reactive consumers with `@ReactiveReliableListener`:

```java
@ReactiveReliableListener("order.created")
public Mono<Void> handle(ReliableMessage<OrderCreatedEvent> message) {
    return orderService.handle(message.payload());
}
```

Milestone 08 supports only handlers with:

```text
Mono<Void> return type
one ReliableMessage<T> parameter
```

Blocking `void` handlers are rejected by the WebFlux listener registrar.

## Reactive Stores

The starter exposes reactive store interfaces for future storage modules:

```java
public interface ReactiveIdempotencyStore {

    Mono<IdempotencyStartResult> tryStart(String key, Duration ttl);

    Mono<Void> markSuccess(String key);

    Mono<Void> markFailed(String key, Throwable error);
}
```

```java
public interface ReactiveOutboxStore {

    Mono<Void> save(OutboxMessage message);

    Flux<OutboxMessage> findPending(int limit);

    Mono<Void> markPublished(String id);

    Mono<Void> markFailed(String id, Throwable error, Instant nextRetryAt);
}
```

## Reactor Context

Use `ReliableMessageReactorContext` to write consumed message metadata into Reactor Context and apply it to downstream publish options:

```java
return ReliableMessageReactorContext.writeMessage(
    businessHandler.handle(message)
        .then(Mono.deferContextual(context -> reactivePublisher.publish(
            "order.processed",
            event,
            ReliableMessageReactorContext.applyTo(PublishOptions.empty(), context)
        ))),
    message
);
```

The helper propagates message, correlation id, trace id, and headers without calling `block()`.

## Completed Milestone

- `reliable-message-webflux-starter` module
- `ReactiveReliablePublisher`
- `@ReactiveReliableListener`
- `ReactiveIdempotencyStore`
- `ReactiveOutboxStore`
- `Mono<Void>` listener validation
- Reactor Context propagation helpers
- Verification that the starter does not bring JDBC onto its classpath
