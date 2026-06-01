# Rabbit RPC WebFlux Bridge Example

Use this module for RabbitMQ request/response from a WebFlux application. It is an RPC bridge with a WebFlux-friendly `Mono` boundary, not event messaging and not fully reactive RabbitMQ.

## Module

```xml
<dependency>
  <groupId>io.github.huynhngochuyhoang</groupId>
  <artifactId>reliable-message-rpc-rabbit-webflux-bridge</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
```

The module uses `AsyncRabbitTemplate` only. It does not use `RabbitTemplate`, event outbox, event retry queues, or DLQ as the normal RPC flow.

## Required AsyncRabbitTemplate Bean

The RPC bridge auto-configuration requires an `AsyncRabbitTemplate` bean. Add Spring AMQP infrastructure and declare the bean:

```xml
<dependency>
  <groupId>org.springframework.boot</groupId>
  <artifactId>spring-boot-starter-amqp</artifactId>
</dependency>
```

```java
@Configuration
class RabbitRpcConfiguration {

    @Bean
    Jackson2JsonMessageConverter rabbitRpcMessageConverter(ObjectMapper objectMapper) {
        return new Jackson2JsonMessageConverter(objectMapper);
    }

    @Bean
    AsyncRabbitTemplate asyncRabbitTemplate(
            RabbitTemplate rabbitTemplate,
            Jackson2JsonMessageConverter rabbitRpcMessageConverter
    ) {
        rabbitTemplate.setMessageConverter(rabbitRpcMessageConverter);
        return new AsyncRabbitTemplate(rabbitTemplate);
    }
}
```

Spring Boot configures the `RabbitTemplate` and Rabbit connection infrastructure from `spring.rabbitmq.*`. The RPC bridge consumes the `AsyncRabbitTemplate`; it does not create one. The `convertSendAndReceiveAsType(...)` API requires a `SmartMessageConverter`, so the DTO, generic response, and envelope examples use `Jackson2JsonMessageConverter` explicitly.

## Required RPC Topology

The RPC bridge does not declare exchanges or responder queues. Provision the configured RPC exchange, responder queue, and routing-key binding through your broker platform or application topology configuration before sending requests.

For the configuration below, declare a direct exchange named `app.rpc` and bind each responder queue to its route, such as `orders.lookup` or `orders.search`.

## Platform Executor Configuration

```yaml
message:
  reliability:
    rpc:
      rabbit:
        webflux:
          enabled: true
          exchange: app.rpc
          default-timeout: 2s
          response-mode: raw
          executor-mode: platform
          executor-threads: 8
          executor-queue-capacity: 256
          max-concurrency: 64
          max-attempts: 2
          retry-backoff:
            - 100ms
            - 500ms
```

The platform queue is bounded. Saturation fails fast with `RabbitRpcBridgeRejectedException`.

## Virtual-Thread Executor Configuration

```yaml
message:
  reliability:
    rpc:
      rabbit:
        webflux:
          enabled: true
          exchange: app.rpc
          default-timeout: 2s
          executor-mode: virtual-thread
          max-concurrency: 64
          max-attempts: 2
          retry-backoff:
            - 100ms
```

Virtual threads reduce blocking cost. They do not make Spring AMQP reactive and do not remove the `max-concurrency` limit.

## Simple Request/Response

```java
@Service
class OrderLookupClient {

    private final ReactiveRabbitRpcClient rpcClient;

    OrderLookupClient(ReactiveRabbitRpcClient rpcClient) {
        this.rpcClient = rpcClient;
    }

    Mono<OrderResponse> find(String orderId) {
        return rpcClient.request("orders.lookup", new OrderLookupRequest(orderId), OrderResponse.class);
    }
}
```

Request creation is lazy and subscription-driven. The bridge offloads `AsyncRabbitTemplate` invocation from the caller/event-loop thread onto its dedicated RPC executor.

## Generic Response Type

Use `ParameterizedTypeReference<T>` when the response is generic:

```java
ParameterizedTypeReference<List<OrderResponse>> responseType =
    new ParameterizedTypeReference<>() {};

return rpcClient.request(
    "orders.search",
    new OrderSearchRequest(customerId),
    responseType,
    RpcOptions.raw()
);
```

## Response Envelope

Envelope mode is explicit. Use it only when the responder returns `RpcResponseEnvelope<T>`.

Successful responder reply:

```java
return RpcResponseEnvelope.success(new OrderResponse(orderId, "PAID"));
```

Client request:

```java
return rpcClient.request(
    "orders.lookup",
    new OrderLookupRequest(orderId),
    OrderResponse.class,
    RpcOptions.envelope()
);
```

Remote error reply:

```java
return RpcResponseEnvelope.error("ORDER_NOT_FOUND", "Order missing", "NotFound");
```

An envelope with `ERROR` status maps to `RabbitRpcRemoteException`. It is an application-level RPC error protocol, not Rabbit event DLQ behavior.

## Per-Request Timeout

Override the configured default timeout when needed:

```java
return rpcClient.request(
    "orders.lookup",
    new OrderLookupRequest(orderId),
    OrderResponse.class,
    RpcOptions.raw().withTimeout(Duration.ofMillis(500))
);
```

Timeout is caller-visible. Cancellation attempts to cancel the client-side future where possible, but timeout or cancellation may not stop broker-side or remote work.

## Retry And Bulkhead

RPC retry is bounded by `max-attempts` and `retry-backoff`. Retry applies to Reactor timeout, native `AmqpReplyTimeoutException`, and failures whose root cause is `IOException`. Other Spring AMQP exceptions are not retried by default. Remote `ERROR` envelopes and reply conversion failures are not retried by default.

The RPC executor `max-concurrency` guard is the bounded fail-fast bulkhead. No unbounded queueing or block-caller mode is added. Rabbit RPC circuit-breaker integration is not implemented.

Retrying a non-idempotent RPC can duplicate downstream side effects. Use RPC retry only when the operation and responder semantics allow it.

## Metrics

RPC metrics are separate from Rabbit event bridge metrics:

```text
rpc_rabbit_requests_total
rpc_rabbit_success_total
rpc_rabbit_failed_total
rpc_rabbit_timeout_total
rpc_rabbit_retry_total
rpc_rabbit_bulkhead_rejected_total
rpc_rabbit_duration
```

Tags:

```text
runtime=webflux
transport=rabbit
rpc_client=default
route=<route>
status=<status>
executor_mode=platform|virtual-thread
```

## Do Not Do This

- Do not add outbox to normal RPC.
- Do not use `AsyncRabbitTemplate` for event publishing.
- Do not use `RabbitTemplate` in the RPC bridge.
- Do not treat Rabbit event retry queues or DLQ as RPC retry semantics.
- Do not treat virtual threads as unlimited concurrency.
- Do not assume timeout or cancellation stops remote work.
