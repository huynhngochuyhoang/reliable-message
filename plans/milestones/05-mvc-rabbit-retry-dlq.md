# Milestone 5 - MVC Rabbit Retry and DLQ

## Design Reference

- Source: [reliable-message-design.md - 4.6 MVC RabbitMQ Adapter](../../reliable-message-design.md#46-mvc-rabbitmq-adapter)
- Reliability rules: [reliable-message-design.md - Reliability Rules](../../reliable-message-design.md#reliability-rules)
- Roadmap: [reliable-message-design.md - Milestone 5 - MVC Rabbit Retry and DLQ](../../reliable-message-design.md#milestone-5---mvc-rabbit-retry-and-dlq)

## Goal

Make RabbitMQ consumer failures visible and recoverable through retry queues and DLQs.

## Scope

- Rabbit retry queue convention
- Rabbit DLQ convention
- Poison message handling
- Retry count tracking
- DLQ retry
- DLQ discard

## Out Of Scope

- Kafka retry topics
- Admin API UI
- WebFlux Rabbit support

## Deliverables

- Retry topology auto-configuration
- DLQ topology auto-configuration
- Retry strategy honoring configured attempts and backoff
- DLQ service for retry and discard operations
- Tests for retry exhaustion and DLQ routing

## Verification

- Failed messages move through configured retry queues
- Messages exceeding attempts are routed to DLQ
- Retry count is visible
- DLQ retry republishes to the main route
- DLQ discard records an intentional discard

## Status

- [ ] Not started
- [ ] In progress
- [x] Done
