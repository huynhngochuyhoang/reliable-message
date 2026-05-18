# Milestone 6 - Observability and Admin API

## Design Reference

- Observability: [reliable-message-design.md - 6. Observability](../../reliable-message-design.md#6-observability)
- Admin API: [reliable-message-design.md - 7. Admin API](../../reliable-message-design.md#7-admin-api)
- Roadmap: [reliable-message-design.md - Milestone 6 - Observability and Admin API](../../reliable-message-design.md#milestone-6---observability-and-admin-api)

## Goal

Expose operational signals and internal controls for message reliability workflows.

## Scope

- `reliable-message-observability`
- `reliable-message-admin-api`
- Micrometer metrics
- OpenTelemetry spans
- MDC propagation
- Admin endpoints
- Dashboard-friendly metric tags

## Out Of Scope

- Public user-facing UI
- Unprotected admin endpoints
- WebFlux-specific admin implementation unless MVC path is stable

## Deliverables

- Metrics for publish, consume, retry, DLQ, duplicate, outbox, and idempotency flows
- Trace spans for publish, consume, outbox, idempotency, retry, and DLQ
- MVC admin endpoints for outbox, DLQ, and idempotency operations
- Security-disabled-by-default or protected-by-default behavior

## Verification

- Metrics contain runtime, transport, event name, consumer, and status tags
- Trace continuity works from publish through consume where context is available
- Admin endpoints are not exposed insecurely by default
- Outbox and DLQ inspection endpoints return operational records

## Status

- [ ] Not started
- [ ] In progress
- [x] Done
