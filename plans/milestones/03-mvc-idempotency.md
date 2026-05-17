# Milestone 3 - MVC Idempotency

## Design Reference

- Source: [reliable-message-design.md - 4.5 MVC Idempotency](../../reliable-message-design.md#45-mvc-idempotency)
- Roadmap: [reliable-message-design.md - Milestone 3 - MVC Idempotency](../../reliable-message-design.md#milestone-3---mvc-idempotency)

## Goal

Prevent duplicate business processing for blocking MVC consumers.

## Scope

- `reliable-message-idempotency-jdbc`
- `reliable-message-idempotency-redis`
- `IdempotencyStore`
- Consumer idempotency wrapper
- Duplicate detection
- Ack-after-success behavior

## Out Of Scope

- Reactive idempotency
- Outbox publishing
- Admin API operations

## Deliverables

- JDBC idempotency store
- Redis idempotency store
- Idempotency state model: `PROCESSING`, `SUCCESS`, `FAILED`, `EXPIRED`
- Consumer integration before business handler execution
- Tests for duplicate skip and failure handling

## Verification

- First delivery starts processing
- Duplicate successful delivery is acknowledged and skipped
- Failed processing is marked failed
- Business handler is not invoked for duplicates

## Status

- [ ] Not started
- [ ] In progress
- [ ] Done
