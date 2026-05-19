# reliable-message

Opinionated reliability and observability framework for message-driven Spring Boot systems.

This project is not an exactly-once messaging framework. It is a toolkit for building services that need practical message reliability patterns:

- effectively-once processing
- outbox publishing
- idempotent consumers
- retry conventions
- dead-letter handling
- tracing, metrics, and correlation propagation
- internal admin operations for outbox, DLQ, and idempotency state

## What We Are Building

The framework is split into two runtime stacks:

- Spring MVC / blocking services
- Spring WebFlux / reactive services

The initial production target is:

```text
MVC + RabbitMQ + JDBC outbox + idempotency + observability
```

After that is stable, the roadmap expands to MVC Kafka, then WebFlux Kafka with R2DBC and Reactive Redis.

See [docs/mvc-rabbit-milestone-01-06.md](docs/mvc-rabbit-milestone-01-06.md) for the completed Milestone 01-06 MVC Rabbit documentation.
See [docs/mvc-kafka-milestone-07.md](docs/mvc-kafka-milestone-07.md) for the completed Milestone 07 MVC Kafka documentation.

## Project Environment

- JDK: 21
- Maven: 3.8.x
- Spring Boot: 3.5.x

## Current Milestone

Milestone 07 adds blocking MVC Kafka support alongside the existing MVC Rabbit path.

Current modules:

- `reliable-message-core`
- `reliable-message-mvc-api`
- `reliable-message-idempotency-jdbc`
- `reliable-message-idempotency-redis`
- `reliable-message-outbox-jdbc`
- `reliable-message-rabbit-mvc`
- `reliable-message-kafka-mvc`
- `reliable-message-observability`
- `reliable-message-admin-api`
- `reliable-message-mvc-starter`

`reliable-message-core` is the runtime-neutral API shared by all future stacks.

It contains:

- `ReliableMessage`
- `PublishOptions`
- common message headers
- serializer abstraction
- retry metadata
- error model
- message status model
- dead-letter record model

The core module must not depend on Spring MVC, WebFlux, JDBC, R2DBC, RabbitMQ, Kafka, or Redis.

`reliable-message-mvc-starter` provides the blocking MVC programming model and pulls in the RabbitMQ and Kafka MVC adapters.
It also brings the JDBC outbox, JDBC idempotency provider, observability module, and disabled-by-default admin API for the MVC stack.

For idempotency, applications should normally use one provider module. The MVC starter already includes `reliable-message-idempotency-jdbc`; `reliable-message-idempotency-redis` is an alternative provider for applications that intentionally store idempotency state in Redis.

## MVC RabbitMQ MVP

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

Configuration:

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
```

## Roadmap Tracking

See [plans/README.md](plans/README.md) for milestone tracking docs linked back to the design source.
