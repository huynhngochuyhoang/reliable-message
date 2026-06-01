# reliable-message

Practical reliability tools for message-driven Spring Boot services.

`reliable-message` helps services publish and consume messages with safer operational defaults:

- outbox publishing
- idempotent consumers
- retry and dead-letter conventions
- correlation, tracing, and metrics
- optional RPC propagation
- optional compliance audit hooks

It is not an exactly-once messaging framework. It provides the patterns teams usually need to build effectively-once workflows on top of RabbitMQ, Kafka, JDBC, R2DBC, Redis, MVC, and WebFlux.

## Quick Start

For architecture diagrams and usage, start here:

- [Architecture flow](docs/architecture-flow.md)
- [Usage guide](docs/usage-guide.md)
- [Rabbit WebFlux blocking bridge](docs/rabbit-webflux-bridge-usage.md)
- [Rabbit RPC WebFlux example](docs/examples/rabbit-rpc-webflux.md)

## Choose A Stack

| Application style | Recommended path |
| --- | --- |
| Spring MVC + RabbitMQ | `reliable-message-mvc-starter` |
| Spring MVC + Kafka | `reliable-message-mvc-starter` + `reliable-message-kafka-mvc` |
| Spring WebFlux + Kafka | `reliable-message-webflux-starter` + `reliable-message-kafka-webflux` |
| Spring WebFlux + RabbitMQ | `reliable-message-rabbit-webflux-bridge` blocking bridge |
| WebFlux persistence | R2DBC or Reactive Redis modules |
| Rabbit request/reply from WebFlux | `reliable-message-rpc-rabbit-webflux-bridge` RPC bridge |
| HTTP RPC propagation | `reliable-message-rpc-mvc` or `reliable-message-rpc-webflux` |
| Audit logging | `reliable-message-audit-mvc` or `reliable-message-audit-webflux` |

RabbitMQ is production-oriented for MVC. WebFlux services should prefer Kafka for reactive messaging. If a WebFlux service must use RabbitMQ, use the Rabbit WebFlux blocking bridge as hybrid mode and migration support; it is not native Reactor RabbitMQ.

## MVC Example

Add the starter:

```xml
<dependency>
  <groupId>io.github.huynhngochuyhoang</groupId>
  <artifactId>reliable-message-mvc-starter</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
```

Publish:

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

Consume:

```java
@ReliableListener("order.created")
public void handle(ReliableMessage<OrderCreatedEvent> message) {
    orderService.handle(message.payload());
}
```

## WebFlux Example

Publish:

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

Consume:

```java
@ReactiveReliableListener("order.created")
public Mono<Void> handle(ReliableMessage<OrderCreatedEvent> message) {
    return orderService.handle(message.payload());
}
```

WebFlux listener methods must return `Mono<Void>`.

## Modules

Core messaging:

- `reliable-message-core`
- `reliable-message-mvc-api`
- `reliable-message-mvc-starter`
- `reliable-message-webflux-starter`

Transports:

- `reliable-message-rabbit-mvc`
- `reliable-message-kafka-mvc`
- `reliable-message-kafka-webflux`
- `reliable-message-rabbit-webflux-bridge`
- `reliable-message-rpc-rabbit-webflux-bridge`

Storage:

- `reliable-message-outbox-jdbc`
- `reliable-message-outbox-r2dbc`
- `reliable-message-idempotency-jdbc`
- `reliable-message-idempotency-r2dbc`
- `reliable-message-idempotency-redis`
- `reliable-message-idempotency-redis-reactive`

Operations and extensions:

- `reliable-message-observability`
- `reliable-message-admin-api`
- `reliable-message-rpc-core`
- `reliable-message-rpc-mvc`
- `reliable-message-rpc-webflux`
- `reliable-message-audit-core`
- `reliable-message-audit-mvc`
- `reliable-message-audit-webflux`

## Documentation

- [Architecture flow](docs/architecture-flow.md)
- [Usage guide](docs/usage-guide.md)
- [Rabbit WebFlux blocking bridge](docs/rabbit-webflux-bridge-usage.md)
- [Rabbit RPC WebFlux example](docs/examples/rabbit-rpc-webflux.md)
- [MVC RabbitMQ](docs/mvc-rabbit-milestone-01-06.md)
- [MVC Kafka](docs/mvc-kafka-milestone-07.md)
- [WebFlux core](docs/webflux-core-milestone-08.md)
- [WebFlux storage](docs/webflux-storage-milestone-09.md)
- [WebFlux Kafka](docs/webflux-kafka-milestone-10.md)
- [RPC extension](docs/rpc-extension-milestone-12.md)
- [Audit extension](docs/audit-extension-milestone-13.md)
- [Roadmap plans](plans/README.md)

## Requirements

- JDK 21
- Maven 3.8+
- Spring Boot 3.5.x

## Verify

```bash
mvn test
```
