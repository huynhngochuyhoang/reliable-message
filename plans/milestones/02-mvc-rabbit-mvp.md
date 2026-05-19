# Milestone 2 - MVC Rabbit MVP

## Design Reference

- Source: [reliable-message-design.md - 4. Stack A - MVC / Blocking Design](../../reliable-message-design.md#4-stack-a---mvc--blocking-design)
- Rabbit adapter: [reliable-message-design.md - 4.6 MVC RabbitMQ Adapter](../../reliable-message-design.md#46-mvc-rabbitmq-adapter)
- Roadmap: [reliable-message-design.md - Milestone 2 - MVC Rabbit MVP](../../reliable-message-design.md#milestone-2---mvc-rabbit-mvp)

## Goal

Provide the first usable blocking Spring MVC RabbitMQ publishing and consuming path.

## Scope

- `reliable-message-mvc-starter`
- `reliable-message-rabbit-mvc`
- `ReliablePublisher`
- `@ReliableListener`
- Rabbit publisher
- Rabbit listener container
- JSON serialization
- Basic metrics
- Correlation ID propagation

## Out Of Scope

- JDBC outbox
- Idempotency stores
- Retry queues and DLQ handling
- Kafka support
- WebFlux support

## Deliverables

- MVC starter auto-configuration
- Rabbit publisher implementation
- Listener discovery for simple `void` handlers
- Message envelope serialization and deserialization
- Sample configuration properties

## Verification

- Publish a `ReliableMessage` to RabbitMQ
- Consume a message through `@ReliableListener`
- Handler is acknowledged only after successful execution
- Correlation ID is copied into message headers and logging context where applicable

## Status

- [ ] Not started
- [ ] In progress
- [x] Done
