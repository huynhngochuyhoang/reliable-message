# Changelog

## Unreleased

Milestone 14 stabilizes the WebFlux RabbitMQ direction while keeping event messaging and RPC separate.

- Added the Rabbit WebFlux blocking bridge for event messaging through `RabbitTemplate`, with platform and virtual-thread executor modes, fail-fast bounded concurrency, Strategy A listener ack-after-`Mono` completion, idempotency integration, event-loop safety warnings, and bridge metrics.
- Added reactive R2DBC outbox flushing for WebFlux event publishers. The flusher reads claimed rows, publishes through the active `ReactiveReliablePublisher`, marks rows published only after successful publish, and marks failures with retry metadata.
- Added R2DBC outbox schema configuration for `text` and `json` storage modes, explicit column type overrides, dialect defaults, PostgreSQL `json/jsonb` binding, and a clear fail-fast boundary for planned binary storage.
- Added R2DBC outbox claim strategy support with a LIMIT-based conditional-update fallback for supported dialects and a PostgreSQL `FOR UPDATE SKIP LOCKED` optimized strategy.
- Added the separate Rabbit RPC WebFlux bridge module based on `AsyncRabbitTemplate`, with `Mono` request/reply, raw and envelope response modes, `ParameterizedTypeReference<T>` support, timeout, bounded retry, bounded fail-fast bulkhead behavior, platform and virtual-thread executor modes, and RPC-specific metrics.
- Updated user-facing docs and examples to describe the Rabbit WebFlux path as a blocking bridge, hybrid mode, and migration support. The docs avoid fully reactive RabbitMQ, exactly-once, and RPC-outbox claims.

Current limitations:

- Rabbit WebFlux event bridge is not fully reactive RabbitMQ and does not provide non-blocking RabbitMQ broker I/O.
- Strategy B async Rabbit ack coordination is not implemented.
- Binary R2DBC outbox payload storage is planned and fails fast today.
- MySQL, Oracle, and SQL Server optimized R2DBC outbox claim strategies are not implemented.
- Rabbit RPC circuit-breaker integration is not implemented.
