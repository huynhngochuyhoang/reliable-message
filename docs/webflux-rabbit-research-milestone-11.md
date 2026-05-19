# WebFlux Rabbit Research

This document records the Milestone 11 decision for RabbitMQ support in the WebFlux stack.

## Decision

Do not add a production `reliable-message-rabbit-webflux` module yet.

RabbitMQ WebFlux support may be added later as an experimental module only if it is implemented on Reactor RabbitMQ, not by wrapping Spring AMQP listener containers behind `Mono` or `Flux` APIs.

Default guidance remains:

```text
Use RabbitMQ with the MVC stack.
Use Kafka with the WebFlux stack.
```

## Evidence

Reactor RabbitMQ is the only candidate that fits the WebFlux runtime boundary. Its reference guide describes Reactor RabbitMQ as a reactive API for RabbitMQ using Reactor and the RabbitMQ Java client, with functional APIs and non-blocking backpressure.

Spring AMQP is not the right implementation base for a WebFlux Rabbit module. Its listener model is container-driven: the container bridges queues to listener callbacks and runs as a lifecycle component. That model can be reliable, but exposing it as `Mono` or `Flux` would hide blocking/container semantics instead of providing a genuinely reactive transport path.

RabbitMQ Java client behavior also matters. Consumer callbacks are dispatched on a client-managed thread pool, and the client documentation explicitly discusses safe use of blocking channel/connection methods from those callbacks. That is compatible with the MVC adapter, but it does not satisfy the WebFlux rule that framework reactive code must avoid blocking paths.

## Reactor RabbitMQ Fit

Reactor RabbitMQ provides the primitives needed for a future experimental adapter:

- `Sender#sendWithPublishConfirms` returns confirmation results that can be composed in a reactive publisher flow.
- `Receiver#consumeManualAck` exposes deliveries that can be acknowledged only after downstream processing completes.
- `ConsumeOptions.qos` provides a RabbitMQ prefetch control that can map to the WebFlux stack's backpressure and concurrency settings.
- The default sender uses RabbitMQ Java client NIO and Reactor schedulers for connection/resource work.

The important caveat is topology/resource management. Reactor RabbitMQ Javadocs mark AMQP RPC-based operations such as exchange/queue declare, bind, unbind, and delete as potentially blocking because AMQP 0-9-1 lacks a request correlation ID for those RPCs. A future adapter should keep topology declaration out of the hot message path and document that startup/admin operations may use bounded elastic scheduler resources.

## Proposed Experimental Module Boundary

If implemented later, the module should be named:

```text
reliable-message-rabbit-webflux
```

The module should:

- depend on `reliable-message-webflux-starter`
- depend on Reactor RabbitMQ
- provide a `ReactiveReliablePublisher` implementation using publisher confirms
- provide a listener container using manual ack
- ack only after the handler `Mono` completes
- use `ReactiveIdempotencyStore` before invoking business logic
- route retry and DLT messages with RabbitMQ-specific exchanges or routing keys
- expose configurable concurrency and prefetch
- avoid Spring AMQP listener containers
- avoid calling `block()` in framework reactive code

## Limitations

- Mark the module experimental until integration tests prove connection recovery, publisher confirms, retry routing, and manual ack behavior under broker failures.
- Do not auto-declare RabbitMQ topology in the hot path.
- Document any startup topology operations as potentially blocking if Reactor RabbitMQ resource APIs are used.
- Do not promise full non-blocking RabbitMQ support when AMQP RPC or Java-client internals are involved.
- Keep MVC RabbitMQ as the stable RabbitMQ recommendation.

## Sources

- Reactor RabbitMQ Reference Guide: https://projectreactor.io/docs/rabbitmq/snapshot/reference/
- Reactor RabbitMQ `Sender` Javadocs: https://projectreactor.io/docs/rabbitmq/release/api/reactor/rabbitmq/Sender.html
- Spring AMQP asynchronous consumer container documentation: https://docs.spring.io/spring-amqp/reference/amqp/receiving-messages/async-consumer.html
- RabbitMQ Java Client API Guide: https://www.rabbitmq.com/client-libraries/java-api-guide
