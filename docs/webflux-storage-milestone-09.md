# WebFlux Storage Documentation

This document describes the completed Milestone 09 reactive storage providers for WebFlux applications.

## Modules

- `reliable-message-outbox-r2dbc`
- `reliable-message-idempotency-r2dbc`
- `reliable-message-idempotency-redis-reactive`

These modules are opt-in storage providers. As of Milestone 14.8.1, `reliable-message-outbox-r2dbc` also provides an opt-in reactive outbox flusher that publishes pending rows through the active WebFlux event transport publisher. See [Milestone 14.8.1 R2DBC outbox flusher](milestone-14-8-1-r2dbc-outbox-flusher.md).

## R2DBC Outbox

Add the R2DBC outbox module:

```xml
<dependency>
    <groupId>io.github.huynhngochuyhoang</groupId>
    <artifactId>reliable-message-outbox-r2dbc</artifactId>
    <version>0.1.0-SNAPSHOT</version>
</dependency>
```

`R2dbcOutboxStore` implements `ReactiveOutboxStore`:

```java
Mono<Void> save(OutboxMessage message);

Flux<OutboxMessage> findPending(int limit);

Mono<Void> markPublished(String id);

Mono<Void> markFailed(String id, Throwable error, Instant nextRetryAt);
```

Schema initialization is exposed as a reactive operation:

```java
return r2dbcOutboxStore.initializeSchema();
```

Production applications should normally manage the schema with migrations.

## Reactive Transactions

`R2dbcOutboxStore` uses Spring R2DBC `DatabaseClient`, so outbox writes can participate in caller-managed reactive transactions:

```java
return transactionalOperator.execute(status ->
    orderRepository.save(order)
        .then(outboxStore.save(OutboxMessage.pending(
            "order.created",
            event,
            PublishOptions.builder()
                .aggregateId(order.id())
                .idempotencyKey(event.id())
                .partitionKey(order.id())
                .build()
        )))
).then();
```

Do not call `block()` to force outbox writes into a transaction. Compose the returned `Mono`.

## R2DBC Idempotency

Add the R2DBC idempotency module:

```xml
<dependency>
    <groupId>io.github.huynhngochuyhoang</groupId>
    <artifactId>reliable-message-idempotency-r2dbc</artifactId>
    <version>0.1.0-SNAPSHOT</version>
</dependency>
```

`R2dbcIdempotencyStore` implements `ReactiveIdempotencyStore`:

```java
Mono<IdempotencyStartResult> tryStart(String key, Duration ttl);

Mono<Void> markSuccess(String key);

Mono<Void> markFailed(String key, Throwable error);
```

Failed or expired keys can be started again. Processing and successful keys are treated as duplicates.

## Reactive Redis Idempotency

Add the Reactive Redis idempotency module:

```xml
<dependency>
    <groupId>io.github.huynhngochuyhoang</groupId>
    <artifactId>reliable-message-idempotency-redis-reactive</artifactId>
    <version>0.1.0-SNAPSHOT</version>
</dependency>
```

`ReactiveRedisIdempotencyStore` uses `ReactiveStringRedisTemplate`.
It starts missing keys with `setIfAbsent`, returns duplicates for active processing/success states, and can restart failed keys.

## Completed Milestone

- R2DBC outbox schema and store
- R2DBC idempotency schema and store
- Reactive Redis idempotency store
- Auto-configuration for each provider module
- R2DBC transaction participation test with `TransactionalOperator`
- Reactive Redis duplicate detection tests
- No blocking calls in framework reactive source
