package io.github.huynhngochuyhoang.reliablemessage.rpc.rabbit.webflux.bridge;

import io.github.huynhngochuyhoang.reliablemessage.rpc.RpcContext;
import io.github.huynhngochuyhoang.reliablemessage.rpc.RpcHeaders;
import io.github.huynhngochuyhoang.reliablemessage.rpc.RpcTimeoutPolicy;
import io.github.huynhngochuyhoang.reliablemessage.rpc.webflux.ReactiveRpcContext;
import org.springframework.amqp.core.AsyncAmqpTemplate;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.rabbit.AsyncRabbitTemplate;
import org.springframework.core.ParameterizedTypeReference;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class DefaultReactiveRabbitRpcClient implements ReactiveRabbitRpcClient {

    private final AsyncAmqpTemplate asyncRabbitTemplate;
    private final RabbitRpcWebFluxBridgeProperties properties;
    private final Scheduler rpcScheduler;

    public DefaultReactiveRabbitRpcClient(
            AsyncRabbitTemplate asyncRabbitTemplate,
            RabbitRpcWebFluxBridgeProperties properties,
            RabbitRpcBridgeExecutorProvider executorProvider
    ) {
        this((AsyncAmqpTemplate) asyncRabbitTemplate, properties, executorProvider.scheduler());
    }

    DefaultReactiveRabbitRpcClient(
            AsyncAmqpTemplate asyncRabbitTemplate,
            RabbitRpcWebFluxBridgeProperties properties,
            Scheduler rpcScheduler
    ) {
        this.asyncRabbitTemplate = Objects.requireNonNull(asyncRabbitTemplate, "asyncRabbitTemplate must not be null");
        this.properties = Objects.requireNonNull(properties, "properties must not be null");
        this.rpcScheduler = Objects.requireNonNull(rpcScheduler, "rpcScheduler must not be null");
    }

    @Override
    public <T> Mono<T> request(String route, Object request, Class<T> responseType) {
        if (route == null || route.isBlank()) {
            throw new IllegalArgumentException("route must not be blank");
        }
        Objects.requireNonNull(responseType, "responseType must not be null");
        RpcTimeoutPolicy timeoutPolicy = new RpcTimeoutPolicy(properties.getDefaultTimeout());

        return Mono.deferContextual(contextView -> {
            RpcContext rpcContext = ReactiveRpcContext.current(contextView).orElse(RpcContext.empty());
            Map<String, String> headers = headers(rpcContext);
            String correlationId = correlationId(headers);
            headers.putIfAbsent(RpcHeaders.CORRELATION_ID, correlationId);
            MessagePostProcessor postProcessor = message -> withRpcHeaders(message, correlationId, headers);
            ParameterizedTypeReference<T> returnType = ParameterizedTypeReference.forType(responseType);

            Mono<T> result = Mono.defer(() -> {
                CompletableFuture<T> future = asyncRabbitTemplate.convertSendAndReceiveAsType(
                        properties.getExchange(),
                        route,
                        request,
                        postProcessor,
                        returnType
                );

                // Phase 14.10 propagates transport, future-completion, and conversion failures only.
                // TODO: map application-level RPC error envelopes in a later phase.
                return Mono.fromFuture(future)
                        .doOnCancel(() -> future.cancel(true));
            }).subscribeOn(rpcScheduler);

            // Reactor timeout cancels the local future, but RabbitMQ may still process a request already accepted by the broker.
            return result.timeout(timeoutPolicy.requestTimeout());
        });
    }

    private static Map<String, String> headers(RpcContext rpcContext) {
        return new LinkedHashMap<>(RpcHeaders.from(rpcContext));
    }

    private static String correlationId(Map<String, String> headers) {
        String existing = headers.get(RpcHeaders.CORRELATION_ID);
        return existing == null || existing.isBlank() ? UUID.randomUUID().toString() : existing;
    }

    private static Message withRpcHeaders(Message message, String correlationId, Map<String, String> headers) {
        message.getMessageProperties().setCorrelationId(correlationId);
        headers.forEach((name, value) -> {
            if (value != null && !value.isBlank()) {
                message.getMessageProperties().setHeader(name, value);
            }
        });
        return message;
    }
}
