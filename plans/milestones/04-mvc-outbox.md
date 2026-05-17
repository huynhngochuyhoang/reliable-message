# Milestone 4 - MVC Outbox

## Design Reference

- Source: [reliable-message-design.md - 4.4 MVC Outbox](../../reliable-message-design.md#44-mvc-outbox)
- Roadmap: [reliable-message-design.md - Milestone 4 - MVC Outbox](../../reliable-message-design.md#milestone-4---mvc-outbox)

## Goal

Persist messages in the same database transaction as business data before publishing.

## Scope

- `reliable-message-outbox-jdbc`
- JDBC `message_outbox` table
- `OutboxStore`
- Outbox flush scheduler
- Rabbit publisher confirm support
- Mark published or failed
- Retry failed publish

## Out Of Scope

- Kafka outbox publishing
- R2DBC outbox
- Admin API retry endpoints

## Deliverables

- JDBC outbox schema
- JDBC outbox repository
- Transaction-friendly outbox save flow
- Scheduled pending-message publisher
- Publish failure tracking with retry metadata

## Verification

- Outbox row is saved in the business transaction
- Pending rows are published by the flush job
- Successfully published rows are marked published
- Failed publishes store error and next retry time

## Status

- [ ] Not started
- [ ] In progress
- [ ] Done
