# Reliable Message Spring Boot - RabbitMQ WebFlux Blocking Bridge Design

This document describes the supported hybrid mode for:

```text
Spring WebFlux + RabbitMQ + Spring AMQP
```

This is not native Reactor RabbitMQ support and not non-blocking RabbitMQ broker I/O support.

Positioning:

```text
blocking bridge
hybrid mode
migration support
virtual-thread optimized blocking support
```

Non-goals:

```text
native Reactor RabbitMQ
non-blocking RabbitMQ broker I/O
unlimited concurrency
hiding Spring AMQP blocking behavior
```

## Current Status

Milestone 14 event bridge direction is implemented through phases 14.1 to 14.8:

```text
module scaffold and properties
bridge executor abstraction
platform and virtual-thread executor modes
bounded concurrency guard
fail-fast rejection
ReactiveRabbitBridgePublisher
Strategy A listener bridge
ack after handler Mono and markSuccess complete
ReactiveIdempotencyStore duplicate semantics
event-loop safety warning/signal
bridge observability
```

Rabbit RPC is intentionally separate and planned for later phases:

```text
reliable-message-rpc-rabbit-webflux-bridge
ReactiveRabbitRpcClient
AsyncRabbitTemplate request/reply
Mono.fromFuture
RPC timeout/retry/circuit-breaker/bulkhead
```

## 1. Why This Exists

Many systems use:

```text
Spring WebFlux HTTP APIs
+
RabbitMQ messaging through Spring AMQP
```

Reasons:

```text
existing RabbitMQ platform
organization-wide RabbitMQ tooling
migration constraints
operational familiarity
no approved Kafka platform
```

The framework supports this reality honestly by isolating blocking Rabbit work instead of pretending Spring AMQP is reactive.

## 2. Module Boundary

Event bridge module:

```text
reliable-message-rabbit-webflux-bridge
```

Use for:

```text
event publish
event consume
RabbitTemplate
Rabbit listener containers
idempotency
ack/nack
Rabbit-native retry/DLQ hooks
bridge metrics
```

Do not use for:

```text
RPC request/reply
AsyncRabbitTemplate
normal RPC timeout/circuit-breaker semantics
outbox-backed RPC
```

Planned RPC bridge module:

```text
reliable-message-rpc-rabbit-webflux-bridge
```

Use for:

```text
ReactiveRabbitRpcClient
AsyncRabbitTemplate
request/reply
Mono.fromFuture
timeout/retry/circuit-breaker/bulkhead
```

Hard rule:

```text
RabbitTemplate belongs to event messaging.
AsyncRabbitTemplate belongs to Rabbit RPC.
```

## 3. Runtime Model

Recommended stack:

```text
Spring WebFlux
Spring AMQP
RabbitTemplate
SimpleMessageListenerContainer
Reactive Redis or R2DBC idempotency
Java 21 virtual threads optional
Micrometer
OpenTelemetry where configured
```

Hybrid runtime:

```text
Reactive HTTP layer
Reactive business APIs
Blocking RabbitMQ layer isolated by bridge executor/listener boundary
```

Critical rule:

```text
Never execute blocking RabbitMQ work on Netty event-loop threads.
```

Blocking Rabbit operations must run on:

```text
Spring AMQP listener threads
dedicated bounded platform thread executor
dedicated virtual-thread executor with bounded submission/concurrency
dedicated scheduler backed by the bridge executor
```

## 4. Configuration Direction

Example platform mode:

```yaml
message:
  reliability:
    runtime: webflux
    transport: rabbit
    service-name: order-service
    rabbit:
      exchange: app.events
      auto-declare: true
      bridge:
        enabled: true
        executor-mode: platform
        worker-threads: 16
        queue-capacity: 1000
        max-concurrency: 256
        rejection-policy: fail-fast
```

Example virtual-thread mode:

```yaml
message:
  reliability:
    runtime: webflux
    transport: rabbit
    service-name: order-service
    rabbit:
      exchange: app.events
      bridge:
        enabled: true
        executor-mode: virtual-thread
        max-concurrency: 1000
        queue-capacity: 1000
        rejection-policy: fail-fast
```

First-version rejection policy:

```text
fail-fast only
```

Out of first-version scope:

```text
block-caller
drop-and-metric
unbounded queues
unbounded submission
```

## 5. Executor and Scheduler

Core abstraction:

```java
public interface RabbitBridgeExecutorProvider {
    ExecutorService executor();
}
```

Implementations:

```text
PlatformThreadRabbitBridgeExecutorProvider
VirtualThreadRabbitBridgeExecutorProvider
```

Platform mode:

```text
bounded worker count
bounded queue capacity
clear bridge thread names
graceful shutdown
fail-fast rejection when saturated
```

Virtual-thread mode:

```text
named virtual threads
bounded submission before work is accepted
external concurrency guard still required
graceful shutdown
```

Virtual thread semantics:

```text
virtual threads reduce blocking cost
virtual threads are not reactive
virtual threads do not remove backpressure concerns
virtual threads still need concurrency limits
```

Do not use:

```text
Netty event loop
Reactor parallel scheduler by default
ForkJoinPool.commonPool
unbounded platform queue
```

## 6. Concurrency Guard

Core components:

```text
RabbitBridgeConcurrencyGuard
RabbitBridgeRejectedException
```

Flow:

```text
acquire permit before submission
 -> submit blocking bridge work
 -> release permit on success, failure, cancellation or executor rejection
```

If concurrency is exhausted:

```text
fail fast with RabbitBridgeRejectedException
```

The guard prevents:

```text
unbounded RabbitTemplate publishes
unbounded virtual-thread submissions
unbounded queued bridge work
resource exhaustion under slow broker or downstream outage
```

## 7. Reactive Event Publisher Bridge

Main component:

```text
ReactiveRabbitBridgePublisher
```

API:

```java
public interface ReactiveReliablePublisher {
    Mono<Void> publish(String eventName, Object payload, PublishOptions options);
}
```

Publish flow:

```text
subscriber calls publish Mono
 -> serialize ReliableMessage before Rabbit execution
 -> acquire concurrency permit
 -> submit RabbitTemplate.convertAndSend to bridge executor
 -> RabbitTemplate runs on bridge thread only
 -> record success/failure metrics
 -> release permit
 -> complete or error the Mono
```

Important:

```text
RabbitTemplate remains blocking.
Mono provides composition and an explicit offload boundary.
Mono does not make RabbitTemplate non-blocking.
```

Rules:

```text
no Mono.just around blocking publish
no inline RabbitTemplate execution on caller thread
no AsyncRabbitTemplate in event bridge
no outbox integration in Milestone 14 event bridge unless explicitly requested later
```

## 8. Reactive Event Listener Bridge

Main components:

```text
ReactiveRabbitBridgeListenerEndpoint
ReactiveRabbitBridgeListenerMethodInvoker
ReactiveRabbitBridgeListenerRegistrar
ReactiveRabbitBridgeMessageHandler
```

Supported listener shape:

```java
@ReactiveReliableListener("order.created")
public Mono<Void> handle(ReliableMessage<OrderCreatedEvent> message) {
    return orderService.handle(message.payload());
}
```

Strategy A flow:

```text
Spring AMQP listener thread
 -> deserialize ReliableMessage
 -> invoke reactive handler
 -> wait for Mono completion at the bridge boundary
 -> ack only after success
```

Strategy A is intentionally blocking at the listener boundary. It is simple, explicit and predictable.

Not implemented in first version:

```text
Strategy B async ack coordination
fire-and-forget subscribe
advanced retry topology creation
```

## 9. Idempotency and Duplicate Semantics

The listener integrates with `ReactiveIdempotencyStore`.

New message flow:

```text
receive
 -> tryStart
 -> invoke handler Mono
 -> markSuccess after handler Mono completes
 -> ack only after markSuccess succeeds
```

Duplicate SUCCESS:

```text
do not invoke handler
ack message
record duplicate outcome
```

Duplicate PROCESSING or FAILED:

```text
do not invoke handler
do not ack as success
use listener failure path and nack/failure hook
```

Failure flow:

```text
handler or idempotency failure
 -> markFailed where possible
 -> failure hook where configured
 -> nack/requeue according to Rabbit listener failure path
```

Rules:

```text
idempotency failure is not business success
markSuccess failure must not ack as success
handler failure must not ack as success
Reactor retry is not Rabbit business retry
```

## 10. Retry and DLQ

Rabbit event retry/DLQ remains Rabbit-native.

Recommended event flow:

```text
main queue
 -> failure
 -> retry queue with TTL or broker policy
 -> back to main queue
 -> exceed attempts
 -> DLQ
```

Milestone 14 event bridge exposes a minimal failure hook. Retry and DLQ outcome metrics are recorded only when event failure hooks expose concrete outcomes.

The bridge does not implement RPC retry through Rabbit retry queues.

## 11. Event Loop Safety

Blocking Rabbit operations must never run on Netty event-loop threads.

Implemented direction:

```text
simple conservative event-loop thread-name detection
warning or safety signal when publish is called from event-loop-style caller
publish still succeeds when bridge capacity exists
RabbitTemplate still runs only on bridge executor
```

Detected style examples:

```text
reactor-http-nio
reactor-http-epoll
reactor-http-kqueue
reactor-tcp-nio
nioEventLoop
epollEventLoop
kqueueEventLoop
```

This detection is a safety signal, not the core correctness mechanism. Correctness comes from explicit bridge executor isolation and bounded concurrency.

## 12. Observability

Bridge metrics:

```text
message_rabbit_bridge_publish_total
message_rabbit_bridge_consume_total
message_rabbit_bridge_duplicate_total
message_rabbit_bridge_failure_outcome_total
message_rabbit_bridge_executor_rejected_total
message_rabbit_bridge_executor_active
message_rabbit_bridge_executor_queued
```

Required tags:

```text
runtime=webflux-bridge
transport=rabbit
executor_mode=platform|virtual-thread
event_name=order.created
status=success|failure|duplicate|retry|dlq|rejected
```

Metrics must not:

```text
change business flow
swallow publish/consume errors
introduce blocking behavior
create a private unexported registry when no MeterRegistry exists
```

## 13. Rabbit RPC Bridge

`AsyncRabbitTemplate` is Rabbit RPC support, not event messaging support.

Correct RPC flow:

```text
WebFlux request
 -> ReactiveRabbitRpcClient
 -> AsyncRabbitTemplate convertSendAndReceive
 -> CompletableFuture
 -> Mono.fromFuture
 -> timeout/retry/circuit-breaker/bulkhead
 -> response or caller-visible failure
```

RPC reliability features:

```text
timeout
retry for retryable RPC failures
circuit breaker
bulkhead
correlation id
trace propagation
metrics
structured logs
optional audit
```

RPC does not use by default:

```text
outbox
background flush
Rabbit event retry queues
DLQ command storage
eventual response recovery
```

If durability is required, model the workflow as async command/event messaging instead of normal RPC.

## 14. Limitations

Honest limitations:

```text
RabbitMQ layer remains blocking
Spring AMQP listener containers are blocking infrastructure
RabbitTemplate is blocking infrastructure
virtual threads reduce blocking cost but do not make RabbitMQ reactive
backpressure is partial and enforced through bounds/rejection
throughput remains limited by RabbitMQ channels, connections and downstream systems
Strategy A blocks listener/bridge threads until handler Mono completes
```

Do not claim:

```text
native Reactor RabbitMQ
non-blocking broker messaging
identical semantics to Reactor Kafka
exactly-once processing
unlimited concurrency because virtual threads exist
```

## 15. Recommended Use Cases

Suitable for:

```text
existing WebFlux systems using RabbitMQ
migration scenarios
organizations standardized on RabbitMQ
incremental modernization
Java 21 deployments that can use virtual threads responsibly
```

Not recommended for:

```text
greenfield fully reactive event streaming
extreme throughput reactive streaming
systems requiring fully non-blocking broker clients
```

For greenfield reactive messaging:

```text
prefer Kafka + Reactor Kafka
```

## 16. Roadmap Alignment

Current Milestone 14 order:

```text
14.1 event bridge module scaffold and properties
14.2 bridge executor and scheduler
14.3 concurrency guard and fail-fast rejection
14.4 ReactiveRabbitBridgePublisher
14.5 Strategy A listener bridge
14.6 listener failure, duplicate and idempotency semantics
14.7 event-loop protection and overload behavior
14.8 event observability
14.9 Rabbit RPC WebFlux bridge module scaffold
14.10 Rabbit RPC request/response client
14.11 Rabbit RPC retry, circuit breaker, bulkhead and metrics
14.12 documentation and limitations
```

Final recommendation:

```text
Keep WebFlux Rabbit event messaging as a blocking bridge.
Keep Rabbit RPC in a separate AsyncRabbitTemplate module.
Keep outbox/idempotency/retry/DLQ semantics attached to event messaging.
Keep timeout/retry/circuit-breaker/bulkhead semantics attached to RPC.
```
