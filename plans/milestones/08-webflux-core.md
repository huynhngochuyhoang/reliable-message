# Milestone 8 - WebFlux Core

## Design Reference

- Source: [reliable-message-design.md - 5. Stack B - WebFlux / Reactive Design](../../reliable-message-design.md#5-stack-b---webflux--reactive-design)
- Runtime rules: [reliable-message-design.md - Runtime Rules](../../reliable-message-design.md#runtime-rules)
- Reactive rules: [reliable-message-design.md - Reactive Rules](../../reliable-message-design.md#reactive-rules)
- Roadmap: [reliable-message-design.md - Milestone 8 - WebFlux Core](../../reliable-message-design.md#milestone-8---webflux-core)

## Goal

Introduce the reactive API boundary without mixing blocking infrastructure into WebFlux.

## Scope

- `reliable-message-webflux-starter`
- `ReactiveReliablePublisher`
- `@ReactiveReliableListener`
- `ReactiveIdempotencyStore`
- `ReactiveOutboxStore`
- Reactor Context propagation

## Out Of Scope

- JDBC in WebFlux
- Blocking Redis in WebFlux
- RabbitMQ WebFlux production support
- Reactive storage implementations

## Deliverables

- WebFlux starter auto-configuration
- Reactive publisher and listener API contracts
- Listener support for `Mono<Void>` handlers
- Reactive store interfaces
- Reactor Context propagation strategy

## Verification

- No framework reactive code calls `block()`
- WebFlux starter has no JDBC dependency
- Listener handlers require reactive return types
- Reactor Context is preserved through framework chains

## Status

- [ ] Not started
- [ ] In progress
- [ ] Done
