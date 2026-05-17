# Milestone 9 - WebFlux Storage

## Design Reference

- WebFlux outbox: [reliable-message-design.md - 5.4 WebFlux Outbox](../../reliable-message-design.md#54-webflux-outbox)
- WebFlux idempotency: [reliable-message-design.md - 5.5 WebFlux Idempotency](../../reliable-message-design.md#55-webflux-idempotency)
- Reactive rules: [reliable-message-design.md - Reactive Rules](../../reliable-message-design.md#reactive-rules)
- Roadmap: [reliable-message-design.md - Milestone 9 - WebFlux Storage](../../reliable-message-design.md#milestone-9---webflux-storage)

## Goal

Provide non-blocking storage implementations for WebFlux reliability features.

## Scope

- `reliable-message-outbox-r2dbc`
- `reliable-message-idempotency-r2dbc`
- `reliable-message-idempotency-redis-reactive`
- TransactionalOperator support
- No blocking calls

## Out Of Scope

- JDBC fallback
- Blocking Redis fallback
- RabbitMQ WebFlux implementation

## Deliverables

- R2DBC outbox schema and implementation
- R2DBC idempotency store
- Reactive Redis idempotency store
- TransactionalOperator integration guidance
- Tests that exercise reactive flows without blocking

## Verification

- Reactive stores return `Mono` or `Flux`
- No `block()` calls in framework reactive code
- R2DBC outbox save works inside reactive transaction
- Reactive Redis idempotency detects duplicates

## Status

- [ ] Not started
- [ ] In progress
- [ ] Done
