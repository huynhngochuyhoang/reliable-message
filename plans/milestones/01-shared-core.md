# Milestone 1 - Shared Core

## Design Reference

- Source: [reliable-message-design.md - 3. Shared Core](../../reliable-message-design.md#3-shared-core)
- Roadmap: [reliable-message-design.md - Milestone 1 - Shared Core](../../reliable-message-design.md#milestone-1---shared-core)

## Goal

Create the runtime-neutral foundation shared by MVC and WebFlux stacks.

## Scope

- `ReliableMessage`
- `PublishOptions`
- Common message headers
- Serializer abstraction
- Retry metadata
- Error model
- Message status model
- Dead-letter record model

## Out Of Scope

- Spring MVC or WebFlux integration
- JDBC, R2DBC, RabbitMQ, Kafka, or Redis dependencies
- Runtime-specific auto-configuration

## Deliverables

- `reliable-message-core` module
- Public core API classes
- Unit tests for core models and serializer contract
- Minimal package documentation for supported concepts

## Verification

- Core module compiles without runtime-stack dependencies
- Unit tests pass
- Dependency tree does not include Spring MVC, WebFlux, JDBC, R2DBC, RabbitMQ, Kafka, or Redis

## Status

- [ ] Not started
- [ ] In progress
- [x] Done
