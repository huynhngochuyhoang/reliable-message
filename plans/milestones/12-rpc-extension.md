# Milestone 12 - RPC Extension

## Design Reference

- Source: [reliable-message-rpc-extension.md](../../reliable-message-rpc-extension.md)

## Goal

Add optional RPC reliability and observability support after the messaging milestones are complete.

## Scope

- `reliable-message-rpc-mvc`
- `reliable-message-rpc-webflux`
- HTTP and gRPC correlation propagation
- RPC retry and timeout conventions
- RPC metrics and tracing conventions
- Circuit breaker integration points

## Out Of Scope

- Universal transport abstraction
- Replacing RabbitMQ or Kafka messaging APIs
- Service discovery
- Load balancing
- Service mesh features

## Deliverables

- MVC RPC module for `RestClient`, blocking `WebClient`, and blocking gRPC stubs
- WebFlux RPC module for `WebClient` and reactive gRPC stubs
- Shared header propagation conventions
- Metrics for RPC request, failure, timeout, retry, and duration
- Trace propagation across HTTP, gRPC, RabbitMQ, and Kafka boundaries

## Verification

- HTTP and gRPC calls propagate correlation headers
- RPC retries respect retryable exception classification
- RPC timeouts are reported as timeout metrics
- Messaging APIs remain separate from RPC APIs
- WebFlux RPC code does not block

## Status

- [ ] Not started
- [ ] In progress
- [ ] Done
