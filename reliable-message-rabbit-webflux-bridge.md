# Reliable Message Spring Boot - RabbitMQ WebFlux Blocking Bridge Design

This document describes the supported hybrid mode for:

```text
Spring WebFlux + RabbitMQ + Spring AMQP
```

This is NOT fully reactive RabbitMQ support.

This design exists because:
- many real production systems already use WebFlux
- RabbitMQ infrastructure already exists
- Spring AMQP remains the dominant RabbitMQ integration in Spring
- no stable Reactor-native RabbitMQ ecosystem currently exists
- Java 21 virtual threads make blocking bridge execution more practical

This module should be positioned as:

```text
legacy support
migration support
hybrid mode
blocking bridge
```

NOT:

```text
fully reactive RabbitMQ
```

---

# 1. Why This Exists

Many systems today look like:

```text
Spring WebFlux HTTP APIs
+
RabbitMQ messaging via Spring AMQP
```

Reasons:
- historical architecture
- organization-wide RabbitMQ adoption
- operational familiarity
- migration constraints
- no approved Kafka platform
- existing Rabbit topology and tooling

In practice, teams often:
- use WebFlux for HTTP
- still use RabbitTemplate
- still use Spring AMQP listener containers

This framework should support that reality honestly.

---

# 2. Important Positioning

This module is:

```text
WebFlux + blocking Rabbit bridge
```

NOT:

```text
fully non-blocking RabbitMQ support
```

The framework must not pretend that:
- RabbitTemplate is reactive
- Spring AMQP listener containers are reactive
- wrapping blocking calls in Mono magically makes them non-blocking
- virtual threads make blocking I/O reactive

Instead:
- isolate blocking work
- protect event loop threads
- optionally use Java 21 virtual threads for blocking work
- document limitations clearly
- provide operational consistency

---

# 3. Module Name

Recommended module:

```text
reliable-message-rabbit-webflux-bridge
```

Do NOT use:

```text
reliable-message-rabbit-webflux
```

because that name incorrectly suggests fully reactive RabbitMQ support.

---

# 4. Recommended Stack

```text
Spring WebFlux
Spring AMQP
RabbitTemplate
SimpleMessageListenerContainer
Reactive Redis
R2DBC
Java 21 virtual threads optional
Micrometer
OpenTelemetry
```

This creates a hybrid runtime:

```text
Reactive HTTP layer
Blocking RabbitMQ layer
```

The framework responsibility is:
- isolate blocking boundaries
- preserve observability
- preserve reliability guarantees
- prevent blocking work from reaching Netty event loop threads

---

# 5. Main Principle

Critical rule:

```text
Never execute blocking RabbitMQ work on Netty event loop threads.
```

Blocking Rabbit operations must run on one of:

```text
dedicated listener threads
dedicated bounded platform thread pool
dedicated virtual thread executor
dedicated scheduler backed by controlled executor
```

---

# 6. Java 21 Virtual Thread Support

Java 21 virtual threads are a strong fit for this bridge because RabbitMQ Spring AMQP operations are blocking I/O.

Virtual threads can reduce the cost of blocking compared to a traditional platform thread pool.

Recommended support:

```text
platform-thread-pool
virtual-thread
```

Config:

```yaml
message:
  reliability:
    rabbit:
      bridge:
        executor-mode: virtual-thread # platform | virtual-thread
        max-concurrency: 1000
        queue-capacity: 10000
```

Important:

```text
virtual threads improve blocking scalability
virtual threads do not make RabbitTemplate reactive
virtual threads do not remove the need for concurrency limits
virtual threads do not replace backpressure
```

Use virtual threads for:
- blocking RabbitTemplate publish
- blocking wait for reactive handler completion in bridge mode
- blocking audit sink only if explicitly configured for MVC/blocking style

Do not use virtual threads for:
- Netty event loop
- Reactor operator execution by default
- CPU-heavy work
- unbounded concurrency

---

# 7. Executor Strategy

The bridge should provide an abstraction:

```java
public interface RabbitBridgeExecutorProvider {

    ExecutorService getExecutor();

}
```

Implementations:

```text
PlatformThreadRabbitBridgeExecutorProvider
VirtualThreadRabbitBridgeExecutorProvider
```

Java 21 virtual thread implementation:

```java
Executors.newVirtualThreadPerTaskExecutor()
```

Platform thread implementation:

```java
ThreadPoolExecutor
```

Recommended default:

```text
platform-thread-pool
```

Recommended for Java 21 deployments:

```text
virtual-thread
```

But only when:
- the team understands blocking semantics
- concurrency limits are configured
- metrics are monitored

---

# 8. Concurrency Guard

Even with virtual threads, the framework must enforce concurrency limits.

Reason:
- RabbitMQ connections/channels are finite
- downstream services are finite
- database pools are finite
- Redis connections are finite
- memory is finite
- broker can be overloaded

Use a semaphore/bulkhead:

```java
Semaphore maxConcurrency
```

Flow:

```text
acquire permit
 -> execute blocking Rabbit work on executor
 -> release permit
```

If concurrency is exhausted:

```text
fail fast
queue with bounded capacity
or apply configured rejection policy
```

Config:

```yaml
message:
  reliability:
    rabbit:
      bridge:
        max-concurrency: 1000
        queue-capacity: 10000
        rejection-policy: fail-fast # fail-fast | block-caller | drop-and-metric
```

Recommended default:

```text
fail-fast
```

Do not allow unbounded queues.

---

# 9. Reactive Publisher Bridge

Main component:

```text
ReactiveRabbitBridgePublisher
```

API:

```java
public interface ReactiveReliablePublisher {

    Mono<Void> publish(
        String eventName,
        Object payload,
        PublishOptions options
    );
}
```

Implementation:

```text
ReactiveReliablePublisher
 -> acquire concurrency permit
 -> offload RabbitTemplate publish
 -> dedicated executor
 -> release permit
 -> return Mono<Void>
```

Conceptual implementation:

```java
Mono.fromRunnable(() ->
    rabbitTemplate.convertAndSend(...)
)
.subscribeOn(rabbitBridgeScheduler);
```

If virtual threads are enabled:

```text
rabbitBridgeScheduler is backed by a virtual-thread executor
```

Important:

```text
Rabbit publish remains blocking internally.
```

The Mono only provides:
- reactive composition
- async boundary
- non-event-loop execution

It does NOT make RabbitTemplate non-blocking.

---

# 10. Dedicated Scheduler

The framework should create a dedicated scheduler:

```java
Scheduler rabbitBridgeScheduler
```

Backing options:

```text
platform thread pool
virtual thread executor
```

Do NOT use:
- Netty event loop
- Reactor parallel scheduler blindly
- unbounded platform thread creation

Example config:

```yaml
message:
  reliability:
    rabbit:
      bridge:
        executor-mode: virtual-thread
        max-concurrency: 1000
        queue-capacity: 10000
```

---

# 11. Reactive Consumer Bridge

Main component:

```text
ReactiveRabbitBridgeListenerContainer
```

Consume flow:

```text
Spring AMQP listener thread
 -> deserialize message
 -> ReactiveIdempotencyStore.tryStart
 -> invoke reactive handler
 -> wait for Mono completion
 -> ack after success
```

Critical rule:

```text
ACK ONLY AFTER Mono COMPLETES SUCCESSFULLY
```

---

# 12. Reactive Listener Example

```java
@ReactiveReliableListener("order.created")
public Mono<Void> handle(
    ReliableMessage<OrderCreatedEvent> message
) {
    return orderService.handle(message.payload());
}
```

The framework internally bridges:

```text
blocking listener thread
 <-> reactive handler pipeline
```

---

# 13. Listener Strategy

There are two possible strategies.

## Strategy A - Block Listener Thread

Flow:

```text
Spring AMQP listener thread
 -> invoke Mono handler
 -> block listener thread until Mono completes
 -> ack
```

Pros:
- simple
- predictable ack semantics
- easier failure handling

Cons:
- listener thread blocked
- lower throughput with platform threads

With Java 21 virtual threads:
- this strategy becomes more attractive
- blocking cost is much lower
- still requires concurrency limits

Recommendation:

```text
Start with Strategy A.
```

Do not over-engineer initial versions.

---

## Strategy B - Async Ack Coordination

Flow:

```text
listener thread
 -> subscribe reactive pipeline
 -> async completion callback
 -> ack later
```

Pros:
- potentially higher throughput
- less thread blocking

Cons:
- significantly more complex
- harder failure semantics
- reconnect handling harder
- ack lifecycle coordination harder

Recommendation:

```text
Consider later only if Strategy A cannot meet throughput requirements.
```

---

# 14. Retry and DLQ

RabbitMQ retry behavior remains Rabbit-native.

Recommended flow:

```text
main queue
 -> fail
 -> retry queue with TTL
 -> back to main queue
 -> exceed attempts
 -> DLQ
```

Do NOT rely on Reactor-only retry for business retries.

Use:
- retry exchanges
- retry queues
- DLQ routing
- retry headers

---

# 15. Reactive Idempotency

Allowed:
- Reactive Redis
- R2DBC

Not recommended:
- JDBC from reactive handler

Consume flow:

```text
listener thread or virtual thread
 -> invoke reactive idempotency
 -> reactive business handler
 -> wait completion
 -> ack
```

---

# 16. WebFlux Compatibility

The framework should still support:
- Reactor Context
- trace propagation
- correlation IDs
- reactive business pipelines

Even though RabbitMQ itself remains blocking.

Target flow:

```text
HTTP request
 -> WebFlux handler
 -> reactive business logic
 -> Rabbit bridge publish
 -> Rabbit bridge consume
 -> reactive downstream handler
```

---

# 17. Observability

Metrics:

```text
message_publish_total
message_publish_failed_total
message_consume_total
message_consume_failed_total
message_retry_total
message_dlq_total
message_duplicate_total
rabbit_bridge_executor_active_tasks
rabbit_bridge_executor_queued_tasks
rabbit_bridge_executor_rejected_tasks
rabbit_bridge_executor_mode
rabbit_bridge_concurrency_in_use
```

Tags:

```text
runtime=webflux-bridge
transport=rabbit
executor_mode=platform|virtual-thread
event_name=order.created
status=success|failed|duplicate|dlq
```

Trace spans:

```text
message.publish.rabbit.bridge
message.consume.rabbit.bridge
message.retry.rabbit
message.dlq.rabbit
```

---

# 18. Event Loop Protection

Critical production requirement:

```text
No Rabbit blocking call may execute on Netty event loop threads.
```

The framework should provide:
- event loop blocking detection
- warnings
- metrics
- scheduler/executor isolation

Possible safety check:

```text
detect if publish occurs on reactor-http-nio thread
emit warning metric/log
```

---

# 19. Limitations

The framework must clearly document limitations.

Examples:

```text
RabbitMQ layer remains blocking
virtual threads reduce blocking cost but do not make RabbitMQ reactive
throughput may still be limited by RabbitMQ channels/connections
backpressure is partial
not equivalent to Reactor Kafka
```

Do NOT claim:

```text
fully reactive RabbitMQ
fully non-blocking messaging
identical semantics to Kafka
unlimited concurrency because virtual threads exist
```

---

# 20. Recommended Use Cases

Suitable for:

```text
existing WebFlux systems using RabbitMQ
migration scenarios
organizations standardized on RabbitMQ
incremental modernization
Java 21 deployments that can use virtual threads
```

Not recommended for:

```text
greenfield reactive event platforms
extreme throughput reactive streaming
systems requiring fully non-blocking messaging
```

For greenfield reactive messaging:

```text
prefer Kafka + Reactor Kafka
```

---

# 21. Failure Handling

The bridge module must handle:
- listener crash
- Mono failure
- executor exhaustion
- virtual thread saturation via external limits
- duplicate delivery
- retry exhaustion
- Rabbit reconnect
- blocking overload

Required:
- metrics
- retry visibility
- DLQ visibility
- executor visibility

---

# 22. Configuration Example

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

Full example:

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

    retry:
      attempts: 5
      backoff:
        - 5s
        - 30s
        - 1m

    idempotency:
      enabled: true
      store: redis-reactive
```

---

# 23. Recommended Roadmap Placement

Roadmap:

```text
Milestone 12 -> RPC Support
Milestone 13 -> Audit Extension
Milestone 14 -> RabbitMQ WebFlux Blocking Bridge
```

Suggested development order:

```text
14.1 executor abstraction
14.2 platform thread executor provider
14.3 virtual thread executor provider
14.4 concurrency guard / bulkhead
14.5 reactive publish bridge
14.6 reactive listener bridge
14.7 ack-after-Mono-completion
14.8 retry/DLQ integration
14.9 reactive idempotency integration
14.10 observability
14.11 load testing with platform threads
14.12 load testing with virtual threads
14.13 limitation documentation
```

---

# 24. Final Recommendation

Current stable recommendations:

```text
MVC + RabbitMQ
MVC + Kafka
WebFlux + Kafka
```

Additional supported hybrid mode:

```text
WebFlux + RabbitMQ Blocking Bridge
```

For Java 21, the bridge should support:

```text
virtual-thread executor mode
```

But it must be positioned honestly:

```text
hybrid mode
blocking bridge
migration support
virtual-thread optimized blocking support
```

The framework should:
- isolate blocking work
- protect event loop threads
- optionally use Java 21 virtual threads
- preserve reliability
- preserve observability

without pretending Spring AMQP is fully reactive.
