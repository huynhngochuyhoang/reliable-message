# Milestone 10 - WebFlux Kafka

## Design Reference

- Source: [reliable-message-design.md - 5.6 WebFlux Kafka Adapter](../../reliable-message-design.md#56-webflux-kafka-adapter)
- Reactive rules: [reliable-message-design.md - Reactive Rules](../../reliable-message-design.md#reactive-rules)
- Roadmap: [reliable-message-design.md - Milestone 10 - WebFlux Kafka](../../reliable-message-design.md#milestone-10---webflux-kafka)

## Goal

Provide production-oriented WebFlux Kafka support using Reactor Kafka.

## Scope

- `reliable-message-kafka-webflux`
- Reactor Kafka publisher
- Reactor Kafka consumer
- Backpressure config
- Commit after `Mono` completion
- Retry topics
- DLT

## Out Of Scope

- RabbitMQ WebFlux
- Blocking listener containers
- JDBC or blocking Redis dependencies

## Deliverables

- Reactive Kafka publisher implementation
- Reactive Kafka listener container
- Configurable max concurrency and prefetch
- Retry topic support
- DLT support
- Offset commit after handler completion

## Verification

- Consumption respects configured concurrency and prefetch
- Offsets commit only after handler `Mono` completes
- Failures propagate through Reactor error signals
- Retry and DLT behavior matches configured attempts and backoff

## Status

- [ ] Not started
- [ ] In progress
- [ ] Done
