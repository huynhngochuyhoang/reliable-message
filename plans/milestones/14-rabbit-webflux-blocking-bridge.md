# Milestone 14 - RabbitMQ WebFlux Blocking Bridge

## Design Reference

- Source: [reliable-message-rabbit-webflux-bridge-v2.md](../../reliable-message-rabbit-webflux-bridge-v2.md)
- Previous design: [reliable-message-rabbit-webflux-bridge.md](../../reliable-message-rabbit-webflux-bridge.md)
- Related research: [Milestone 11 - WebFlux Rabbit Research](11-webflux-rabbit-research.md)
- Related extension: [Milestone 12 - RPC Extension](12-rpc-extension.md)

## Goal

Support existing Spring WebFlux services that must continue using RabbitMQ through Spring AMQP, without pretending RabbitMQ support is fully reactive.

This milestone introduces an honest hybrid mode:

```text
Spring WebFlux HTTP/runtime
+
blocking RabbitMQ Spring AMQP bridge
```

## Positioning

The module is:

- legacy support
- migration support
- hybrid mode
- blocking bridge
- Java 21 virtual-thread friendly
- virtual-thread optimized blocking support

The module is not:

- fully reactive RabbitMQ
- fully non-blocking messaging
- equivalent to Reactor Kafka
- unlimited concurrency because virtual threads exist
- `reliable-message-rabbit-webflux`

## Module Split

Event messaging module:

```text
reliable-message-rabbit-webflux-bridge
```

Uses:

- `RabbitTemplate`
- optional outbox
- Rabbit-native retry and DLQ
- reactive idempotency
- event publish and consume

RPC module:

```text
reliable-message-rpc-rabbit-webflux-bridge
```

Uses:

- `AsyncRabbitTemplate`
- request/reply
- `Mono.fromFuture`
- timeout, retry, circuit breaker, and bulkhead
- metrics, tracing, and correlation IDs

Do not mix `ReliablePublisher` with `ReliableRpcClient`. They have different semantics.

## Event Messaging Scope

- Executor abstraction for blocking Rabbit bridge work
- Platform thread executor provider
- Java 21 virtual thread executor provider
- Dedicated Rabbit bridge scheduler
- Concurrency guard/bulkhead
- Bounded queue capacity and rejection policy
- Reactive publisher bridge implementing `ReactiveReliablePublisher`
- Reactive listener bridge for `@ReactiveReliableListener`
- Strategy A listener handling: block listener or bridge thread until handler `Mono` completes
- Ack only after handler `Mono` completes successfully
- Rabbit-native retry and DLQ integration
- Reactive idempotency through R2DBC or Reactive Redis
- Reactor Context, trace, and correlation propagation
- Event loop protection and warning metrics
- Bridge observability metrics
- Clear limitation documentation

## Rabbit RPC Scope

`AsyncRabbitTemplate` is only for Rabbit RPC request/reply, not event messaging.

Add:

- `ReactiveRabbitRpcClient`
- `RpcOptions`
- request/reply over `AsyncRabbitTemplate`
- `Mono.fromFuture` bridge for replies
- timeout handling
- retry policy
- circuit breaker
- bulkhead
- correlation ID propagation
- trace propagation
- RPC metrics
- optional audit

RPC does not use outbox by default. If a command must be durable, model it as an async command/event workflow instead of normal RPC.

## Out Of Scope

- Fully non-blocking RabbitMQ support
- Reactor RabbitMQ implementation
- Replacing Spring AMQP
- Unbounded virtual-thread usage
- Unbounded queues
- JDBC inside reactive flows
- Reactor-only retry as the primary Rabbit business retry mechanism
- Claiming RabbitMQ and Kafka have identical semantics
- Async ack coordination strategy unless Strategy A cannot meet throughput requirements
- Event publishing through `AsyncRabbitTemplate`
- Outbox-backed Rabbit RPC by default

## Runtime Rules

- Never execute blocking RabbitMQ work on Netty event loop threads.
- RabbitTemplate publish must run on the bridge executor.
- Listener bridge may block the listener/bridge thread while waiting for the handler `Mono`.
- Ack must happen only after the handler `Mono` completes successfully.
- Concurrency must be bounded even when virtual threads are enabled.
- Queues must be bounded.
- Default rejection policy should be `fail-fast`.
- Rabbit event retries must use Rabbit-native retry queues, retry headers, and DLQ routing.
- Virtual threads improve blocking scalability but do not make RabbitMQ reactive.

## Configuration Shape

Platform thread mode:

```yaml
message:
  reliability:
    runtime: webflux
    transport: rabbit
    mode: blocking-bridge
    rabbit:
      exchange: app.events
      bridge:
        enabled: true
        executor-mode: platform
        worker-threads: 16
        queue-capacity: 10000
        max-concurrency: 256
        rejection-policy: fail-fast
```

Virtual thread mode:

```yaml
message:
  reliability:
    runtime: webflux
    transport: rabbit
    mode: blocking-bridge
    rabbit:
      exchange: app.events
      bridge:
        enabled: true
        executor-mode: virtual-thread
        max-concurrency: 1000
        queue-capacity: 10000
        rejection-policy: fail-fast
```

RPC bridge shape:

```yaml
message:
  reliability:
    rpc:
      rabbit:
        webflux-bridge:
          enabled: true
          timeout: 2s
          max-concurrency: 256
          circuit-breaker:
            enabled: true
```

## Deliverables

- `reliable-message-rabbit-webflux-bridge` module
- `RabbitBridgeExecutorProvider`
- `PlatformThreadRabbitBridgeExecutorProvider`
- `VirtualThreadRabbitBridgeExecutorProvider`
- bridge properties and auto-configuration
- bounded concurrency guard
- dedicated bridge scheduler
- `ReactiveRabbitBridgePublisher`
- `ReactiveRabbitBridgeListenerContainer`
- ack-after-`Mono` completion handling
- retry/DLQ integration compatible with existing Rabbit conventions
- reactive idempotency hooks
- Reactor Context and correlation propagation
- event-loop misuse detection
- metrics for executor, publish, consume, retry, DLQ, duplicate, rejection, and event-loop misuse
- documentation that clearly says this is a blocking bridge

RPC deliverables:

- `reliable-message-rpc-rabbit-webflux-bridge` module
- `ReactiveRabbitRpcClient`
- `RpcOptions`
- `AsyncRabbitTemplate` request/reply adapter
- timeout, retry, circuit breaker, and bulkhead integration
- RPC metrics and tracing
- documentation that RPC is request/reply and does not use outbox by default

## Development Order

1. Executor abstraction
2. Platform thread executor provider
3. Virtual thread executor provider
4. Concurrency guard/bulkhead
5. Dedicated bridge scheduler
6. Reactive publish bridge for event messaging
7. Reactive listener bridge for event messaging
8. Ack-after-`Mono` completion
9. Rabbit-native retry/DLQ integration for event messaging
10. Reactive idempotency integration
11. Reactor Context, trace, and correlation propagation
12. Event-loop protection
13. Messaging observability
14. Rabbit RPC bridge with `AsyncRabbitTemplate`
15. RPC timeout/retry/circuit-breaker/bulkhead
16. RPC metrics and tracing
17. Load testing with platform threads
18. Load testing with virtual threads
19. Limitation documentation

## Verification

- Publisher never executes `RabbitTemplate` work on Netty event loop threads.
- Publisher offloads blocking work to the configured bridge executor.
- Platform thread mode enforces worker and queue limits.
- Virtual thread mode still enforces `max-concurrency`.
- Rejection policy is tested for executor/concurrency exhaustion.
- Consumer acks only after handler `Mono` completes.
- Consumer does not ack when handler `Mono` fails.
- Retry/DLQ routing works through Rabbit-native retry conventions.
- Duplicate messages do not invoke the business handler.
- Reactive idempotency works with R2DBC or Reactive Redis.
- Reactor Context, trace, and correlation IDs survive bridge boundaries.
- Metrics expose active tasks, queued tasks, rejected tasks, concurrency in use, publish/consume results, retry, DLQ, duplicate, and event-loop misuse.
- RPC uses `AsyncRabbitTemplate`, not `RabbitTemplate`.
- RPC exposes success, failure, timeout, retry, duration, and circuit-breaker metrics.
- RPC does not use outbox by default.
- Documentation states that virtual threads improve blocking scalability but do not make RabbitMQ reactive.
- Documentation states that this bridge is suitable for migration and existing RabbitMQ systems, while greenfield reactive messaging should prefer Kafka with Reactor Kafka.

## Status

- [ ] Not started
- [ ] In progress
- [ ] Done
