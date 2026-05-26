# Rabbit RPC WebFlux Example

Rabbit RPC WebFlux bridge is planned, not currently implemented in this repo.

The intended module and API from the design are:

```text
reliable-message-rpc-rabbit-webflux-bridge
ReactiveRabbitRpcClient
AsyncRabbitTemplate request/reply
Mono.fromFuture
```

Do not add this dependency until the module exists.

## Current Implemented RPC Support

Current WebFlux RPC support is HTTP/WebClient-oriented:

```xml
<dependency>
  <groupId>io.github.huynhngochuyhoang</groupId>
  <artifactId>reliable-message-rpc-webflux</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
```

```yaml
message:
  reliability:
    rpc:
      webflux:
        enabled: true
        request-timeout: 2s
        max-attempts: 3
        backoff:
          - 100ms
          - 500ms
```

This config applies to the current WebFlux RPC operator and WebClient filter. It is not Rabbit request/reply configuration.

## Planned Rabbit RPC Flow

```text
WebFlux caller
 -> ReactiveRabbitRpcClient
 -> AsyncRabbitTemplate request/reply
 -> CompletableFuture
 -> Mono.fromFuture
 -> timeout/retry/circuit-breaker/bulkhead
 -> response or caller-visible error
```

`AsyncRabbitTemplate` is RPC only. RPC does not use outbox by default.

## Timeout Example For Current WebFlux RPC Operator

For current WebFlux RPC support, configure `request-timeout` and compose calls through WebClient with Reactor Context:

```java
return webClient.get()
    .uri("/customers/{id}", customerId)
    .retrieve()
    .bodyToMono(CustomerResponse.class)
    .contextWrite(ReactiveRpcContext.write(RpcContext.builder()
        .correlationId(correlationId)
        .requestId(requestId)
        .traceId(traceId)
        .build()));
```

`ReactiveRpcOperator` supports Reactor timeout and retry composition. Rabbit-specific `ReactiveRabbitRpcClient`, circuit breaker, and bulkhead behavior are planned for later Rabbit RPC phases.

## Do Not Do This

- Do not add outbox to normal RPC by default.
- Do not use `AsyncRabbitTemplate` for event publishing.
- Do not use `RabbitTemplate` for request/reply RPC.
- Do not treat Rabbit event retry queues or DLQ as RPC retry semantics.
- Do not copy a `ReactiveRabbitRpcClient` snippet until that API is implemented.
