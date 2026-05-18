# Reliable Message Spring Boot - RPC Extension Design

This document describes the optional RPC support phase for the Reliable Message framework.

RPC support is intentionally designed as:
- an optional extension
- a shared observability and reliability layer
- NOT a replacement for event-driven messaging
- NOT a universal transport abstraction

---

# 1. Goal

The framework primarily focuses on:

```text
message reliability
event-driven systems
async processing
```

However, many production systems still use:
- HTTP RPC
- gRPC
- internal service-to-service calls

This extension provides:
- tracing
- correlation propagation
- retry conventions
- timeout conventions
- metrics
- structured logging
- circuit breaker integration

for RPC communication.

---

# 2. Important Design Principle

Do NOT unify RPC and async messaging into one abstraction.

Avoid designs like:

```java
messageClient.send(...)
```

where the implementation might:
- use Kafka
- use RabbitMQ
- use HTTP
- use gRPC

This creates unclear business semantics.

RPC and async messaging are fundamentally different communication models.

---

# 3. Communication Model Difference

## Async Messaging

Characteristics:

```text
fire-and-forget
eventual consistency
retry queues
dead-letter queues
outbox pattern
idempotent consumer
async processing
```

Examples:
- RabbitMQ
- Kafka

---

## RPC

Characteristics:

```text
request-response
latency sensitive
timeout sensitive
synchronous dependency
circuit breaker
connection pooling
backpressure
```

Examples:
- HTTP REST
- gRPC

These two models should remain separate in the framework architecture.

---

# 4. What SHOULD Be Shared

The following concepts are valuable across both:
- HTTP
- gRPC
- RabbitMQ
- Kafka

## Shared Observability

```text
correlation id
trace id
request id
tenant id
structured logging
metrics
distributed tracing
```

Goal:

```text
HTTP request
 -> RPC call
 -> publish event
 -> Kafka consume
 -> downstream RPC call
```

should appear as a single distributed trace.

---

## Shared Header Convention

Recommended headers:

```text
x-correlation-id
x-request-id
x-trace-id
x-tenant-id
```

Supported across:
- HTTP
- gRPC metadata
- Kafka headers
- RabbitMQ headers

---

## Shared Retry Policy Concepts

Concepts may be shared:

```java
RetryPolicy
TimeoutPolicy
BackoffPolicy
```

But implementations must remain transport-specific.

---

# 5. What SHOULD NOT Be Shared

## Do NOT Create Universal Transport APIs

Avoid:

```java
transportClient.send(...)
```

that magically:
- switches between Kafka and gRPC
- hides sync vs async behavior
- hides delivery guarantees

This leads to:
- unclear semantics
- debugging complexity
- broken abstractions

---

## Do NOT Pretend RPC Is Event-Driven

This:

```text
request -> RPC -> response
```

is NOT equivalent to:

```text
publish event -> eventual processing
```

RPC introduces synchronous dependency and latency coupling.

---

## Do NOT Build a Service Mesh Framework

Avoid scope explosion into:
- service discovery
- advanced load balancing
- full RPC framework
- orchestration platform

This framework should remain focused on:
- reliability
- observability
- operational consistency

---

# 6. MVC RPC Extension

Module:

```text
reliable-message-rpc-mvc
```

Target stack:

```text
Spring MVC
RestClient
WebClient (blocking usage)
gRPC blocking stubs
```

---

## MVC RPC Features

### Retry Convention

```text
configurable retries
exponential backoff
retryable exception classification
```

### Timeout Convention

```text
connect timeout
read timeout
overall request timeout
```

### Circuit Breaker Integration

Recommended integrations:
- Resilience4j
- Spring Retry

### Metrics

Suggested metrics:

```text
rpc_client_requests_total
rpc_client_failures_total
rpc_client_duration
rpc_client_timeout_total
rpc_client_retry_total
```

### Trace Propagation

Automatically propagate:
- trace id
- correlation id
- request id

through:
- HTTP headers
- gRPC metadata

---

# 7. WebFlux RPC Extension

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

---

## WebFlux RPC Features

### Reactive Retry

Use Reactor-native retry behavior:

```text
retryWhen
exponential backoff
retry classification
```

### Reactive Timeout

Support:

```text
Mono.timeout(...)
```

with standardized timeout conventions.

### Reactor Context Propagation

Preserve:
- trace id
- correlation id
- request id

through Reactor Context.

### Metrics

Suggested metrics:

```text
rpc_reactive_requests_total
rpc_reactive_failures_total
rpc_reactive_duration
rpc_reactive_timeout_total
rpc_reactive_retry_total
```

### Reactive Backpressure Rules

```text
avoid unbounded flatMap
respect downstream demand
avoid blocking calls
avoid block()
```

---

# 8. Relationship with Messaging

Recommended production architecture:

## Sync Communication

```text
HTTP
gRPC
```

Used for:
- queries
- low-latency operations
- immediate responses
- synchronous orchestration

## Async Communication

```text
RabbitMQ
Kafka
```

Used for:
- domain events
- async jobs
- decoupled processing
- eventual consistency

## Shared Reliability Layer

```text
tracing
correlation
metrics
retry conventions
observability
```

---

# 9. Recommended Roadmap Placement

RPC support should be added only after:
- messaging core is stable
- outbox is stable
- idempotency is stable
- observability is stable
- RabbitMQ support is stable
- Kafka support is stable

Recommended order:

```text
1. MVC RabbitMQ
2. MVC Kafka
3. WebFlux Kafka
4. Observability stabilization
5. Admin tooling stabilization
6. RPC extension
```

RPC support should remain:
- optional
- secondary
- reliability-oriented

not the primary framework capability.

---

# 10. Final Recommendation

This framework should evolve into:

```text
Distributed communication reliability framework
```

with:
- async messaging reliability
- RPC observability
- retry conventions
- trace propagation
- operational consistency

But the core identity should remain:

```text
message reliability framework
```

Avoid:
- magical transport abstraction
- fake broker transparency
- trying to hide sync vs async semantics
- building a service mesh replacement
