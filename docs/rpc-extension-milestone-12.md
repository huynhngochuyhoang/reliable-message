# RPC Extension Documentation

This document describes the completed Milestone 12 RPC extension baseline.

## Modules

- `reliable-message-rpc-core`
- `reliable-message-rpc-mvc`
- `reliable-message-rpc-webflux`

The RPC extension stays separate from RabbitMQ and Kafka APIs. It provides RPC reliability and observability conventions without creating a universal transport abstraction.

## Shared Conventions

`reliable-message-rpc-core` defines the shared propagation headers:

```text
x-correlation-id
x-request-id
x-trace-id
x-tenant-id
```

It also provides:

- `RpcContext`
- `RpcContextHolder` for MVC/thread-local propagation
- `RpcRetryPolicy`
- `RpcTimeoutPolicy`
- `RpcExceptionClassifier`
- `RpcMetrics`

## MVC Support

`reliable-message-rpc-mvc` adds:

- `RpcRestClientInterceptor`
- `RestClientCustomizer` auto-configuration
- `RpcGrpcMetadata` helper for mapping the shared RPC context into gRPC metadata adapters
- request, failure, timeout, retry, and duration metric helpers

MVC propagation source:

```java
RpcContextHolder.set(RpcContext.builder()
    .correlationId(correlationId)
    .requestId(requestId)
    .traceId(traceId)
    .tenantId(tenantId)
    .build());
```

The auto-configured `RestClient` interceptor copies the context into outbound HTTP headers and records client metrics.

## WebFlux Support

`reliable-message-rpc-webflux` adds:

- `ReactiveRpcContext`
- `RpcWebClientExchangeFilter`
- `WebClientCustomizer` auto-configuration
- `ReactiveRpcOperator` for Reactor-native timeout and retry composition

WebFlux propagation source:

```java
return webClient.get()
    .uri("/orders/{id}", orderId)
    .retrieve()
    .bodyToMono(OrderResponse.class)
    .contextWrite(ReactiveRpcContext.write(RpcContext.builder()
        .correlationId(correlationId)
        .requestId(requestId)
        .traceId(traceId)
        .tenantId(tenantId)
        .build()));
```

`ReactiveRpcOperator` applies `Mono.timeout(...)` and `retryWhen(...)` without blocking.

## Current Limitations

- This milestone adds RPC extension points and HTTP propagation hooks.
- gRPC support is currently a metadata/header convention helper, not direct generated-stub interception.
- Circuit breaker integration is an extension point; no Resilience4j dependency is forced.
- Messaging APIs remain separate from RPC APIs.

## Metrics

MVC metrics use the `rpc_client` prefix:

```text
rpc_client_requests_total
rpc_client_failures_total
rpc_client_duration
rpc_client_timeout_total
rpc_client_retry_total
```

WebFlux metrics use the `rpc_reactive` prefix:

```text
rpc_reactive_requests_total
rpc_reactive_failures_total
rpc_reactive_duration
rpc_reactive_timeout_total
rpc_reactive_retry_total
```
