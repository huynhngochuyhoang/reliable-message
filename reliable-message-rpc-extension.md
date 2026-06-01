# Reliable Message Spring Boot - RPC Extension Design

This document describes optional RPC support for the Reliable Message framework.

RPC support is intentionally:

```text
optional
request/response oriented
observability and resilience focused
separate from event messaging
```

It is not a replacement for event-driven messaging and it is not a universal transport abstraction.

## Current Status

The implemented direction keeps event messaging and RPC separate:

```text
ReliablePublisher != ReliableRpcClient
ReactiveReliablePublisher != ReactiveRabbitRpcClient
```

Rabbit-specific split:

```text
RabbitTemplate is event messaging only.
AsyncRabbitTemplate is Rabbit RPC only.
RPC does not use outbox by default.
```

Milestone 14 keeps Rabbit event bridge and Rabbit RPC bridge in separate modules.

## 1. Goal

The framework primarily focuses on:

```text
message reliability
event-driven systems
async processing
effectively-once processing through outbox and idempotency
```

RPC support adds consistent conventions for:

```text
tracing
correlation propagation
timeout
retry
circuit breaker
bulkhead
metrics
structured logging
```

for synchronous service-to-service communication.

## 2. Communication Model Difference

### Event Messaging

Event messaging is for eventual consistency.

```text
fire-and-forget publish
async consume
outbox
idempotent consumer
broker retry queues or retry topics
DLQ/DLT
```

Examples:

```text
RabbitMQ event publish/consume
Kafka topic publish/consume
```

### RPC

RPC is request/response communication.

```text
caller waits for response or timeout
latency sensitive
synchronous dependency
retry requires idempotency awareness
circuit breaker and bulkhead are primary resilience controls
```

Examples:

```text
HTTP REST
gRPC
RabbitMQ request/reply with AsyncRabbitTemplate
```

These models must remain separate in APIs, modules, metrics and documentation.

## 3. What Can Be Shared

The following concepts are useful across HTTP, gRPC, RabbitMQ RPC, Rabbit events and Kafka events:

```text
correlation id
trace id
request id
tenant id
structured logging
metrics
distributed tracing
common retry policy vocabulary
common timeout policy vocabulary
```

Shared headers:

```text
x-correlation-id
x-request-id
x-trace-id
x-tenant-id
```

Supported carriers:

```text
HTTP headers
gRPC metadata
RabbitMQ message properties
Kafka headers
```

Policy concepts may be shared, but implementations remain transport-specific:

```text
RetryPolicy
TimeoutPolicy
BackoffPolicy
ExceptionClassifier
```

## 4. What Must Not Be Shared

Do not create a universal transport API such as:

```java
transportClient.send(...)
```

that hides whether the call is Kafka, Rabbit event messaging, HTTP, gRPC or Rabbit RPC.

Do not pretend RPC is event-driven:

```text
request -> wait -> response
```

is not equivalent to:

```text
publish event -> process eventually
```

Do not use outbox for normal RPC by default. Persisting an RPC request and flushing it later changes it into a durable async command workflow.

## 5. MVC RPC Extension

Module:

```text
reliable-message-rpc-mvc
```

Target stack:

```text
Spring MVC
RestClient
WebClient in blocking usage only when explicitly chosen
gRPC blocking stubs
```

Features:

```text
connect timeout
read timeout
overall request timeout
retry with exception classification
circuit breaker
bulkhead
metrics
trace and correlation propagation
```

Suggested metrics:

```text
rpc_client_requests_total
rpc_client_failures_total
rpc_client_duration
rpc_client_timeout_total
rpc_client_retry_total
rpc_client_bulkhead_rejected_total
rpc_client_circuit_open_total
```

## 6. WebFlux RPC Extension

Module:

```text
reliable-message-rpc-webflux
```

Target stack:

```text
Spring WebFlux
WebClient
reactive gRPC stubs
Reactor
```

Rules:

```text
preserve Reactor Context
avoid blocking calls
avoid block()
avoid unbounded flatMap
avoid unbounded queues
```

Features:

```text
Mono.timeout
Reactor-native retry composition
exception classification
circuit breaker
bulkhead
metrics
trace and correlation propagation
```

Suggested metrics:

```text
rpc_reactive_requests_total
rpc_reactive_failures_total
rpc_reactive_duration
rpc_reactive_timeout_total
rpc_reactive_retry_total
rpc_reactive_bulkhead_rejected_total
rpc_reactive_circuit_open_total
```

## 7. Rabbit RPC WebFlux Bridge

Implemented module:

```text
reliable-message-rpc-rabbit-webflux-bridge
```

This is separate from:

```text
reliable-message-rabbit-webflux-bridge
```

The event bridge uses `RabbitTemplate`. The RPC bridge uses `AsyncRabbitTemplate`.

Primary API:

```java
public interface ReactiveRabbitRpcClient {
    <T> Mono<T> request(
        String route,
        Object request,
        Class<T> responseType,
        RpcOptions options
    );
}
```

Conceptual flow:

```text
WebFlux caller
 -> build RPC request
 -> dedicated RPC bridge executor
 -> AsyncRabbitTemplate convertSendAndReceive
 -> CompletableFuture
 -> Mono boundary
 -> timeout/bounded retry/fail-fast bulkhead
 -> response or caller-visible failure
```

Implemented composition shape:

```java
executorProvider.execute(() -> asyncRabbitTemplate.convertSendAndReceiveAsType(...))
    .timeout(options.timeout());
```

The bridge supports raw replies, explicit `RpcResponseEnvelope<T>` replies, `ParameterizedTypeReference<T>` response types, and platform or virtual-thread executor modes. Both executor modes remain bounded by `max-concurrency`.

Important boundaries:

```text
AsyncRabbitTemplate means Rabbit request/reply.
It is not event publishing.
It is not outbox-backed messaging.
It is not native Reactor RabbitMQ.
```

## 8. RPC Reliability Semantics

RPC outcomes:

```text
success response
timeout
remote failure
reply deserialization failure
bulkhead rejection
broker unavailable
```

RPC retry is request/response retry. It is not Rabbit event retry queue behavior.

Rabbit RPC circuit-breaker integration is not implemented.

Retry risks:

```text
retrying non-idempotent RPC calls can duplicate downstream side effects
timeout cancellation may not cancel broker-side or remote work
remote service may still complete after caller timeout
```

The framework should make these outcomes visible, not hide them behind event messaging semantics.

## 9. Durable Command Exception

Some commands require durable acceptance.

Examples:

```text
bank transfer command
high-value financial command
legal/compliance command
```

If the request must survive caller timeout or process restart, do not model it as normal RPC. Model it as durable async command/event workflow:

```text
save command
publish command event through event messaging
return accepted
process asynchronously
query status later
```

That belongs to event messaging and outbox, not normal RPC.

## 10. Relationship With Messaging

Recommended architecture:

```text
HTTP/gRPC/Rabbit RPC for queries and low-latency request/response
Rabbit/Kafka event messaging for domain events and async workflows
```

Shared observability should connect both paths:

```text
HTTP request
 -> RPC call
 -> publish event
 -> consume event
 -> downstream RPC call
```

But the framework must keep semantics separate:

```text
ReliablePublisher publishes events.
ReliableRpcClient makes requests.
ReactiveReliablePublisher publishes reactive events.
ReactiveRabbitRpcClient makes Rabbit request/reply calls.
```

## 11. Roadmap Placement

RPC support should remain optional and secondary to message reliability.

Recommended order:

```text
1. Stable event messaging core
2. Stable outbox and idempotency
3. Stable observability
4. MVC Rabbit and Kafka
5. WebFlux Kafka
6. Rabbit WebFlux blocking bridge for event messaging
7. Separate Rabbit WebFlux RPC bridge
8. RPC resilience metrics and hardening
```

Final rule:

```text
Do not merge RPC and event messaging abstractions.
Do not use AsyncRabbitTemplate for event publishing.
Do not use outbox for normal RPC by default.
```
