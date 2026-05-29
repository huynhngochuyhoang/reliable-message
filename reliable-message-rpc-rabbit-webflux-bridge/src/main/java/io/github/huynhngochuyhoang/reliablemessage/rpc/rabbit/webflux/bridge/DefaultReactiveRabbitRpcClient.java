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
import org.springframework.core.ResolvableType;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class DefaultReactiveRabbitRpcClient implements ReactiveRabbitRpcClient {

    private final AsyncAmqpTemplate asyncRabbitTemplate;
    private final RabbitRpcWebFluxBridgeProperties properties;
    private final RabbitRpcBridgeExecutorProvider executorProvider;

    public DefaultReactiveRabbitRpcClient(
            AsyncRabbitTemplate asyncRabbitTemplate,
            RabbitRpcWebFluxBridgeProperties properties,
            RabbitRpcBridgeExecutorProvider executorProvider
    ) {
        this((AsyncAmqpTemplate) asyncRabbitTemplate, properties, executorProvider);
    }

    DefaultReactiveRabbitRpcClient(
            AsyncAmqpTemplate asyncRabbitTemplate,
            RabbitRpcWebFluxBridgeProperties properties,
            RabbitRpcBridgeExecutorProvider executorProvider
    ) {
        this.asyncRabbitTemplate = Objects.requireNonNull(asyncRabbitTemplate, "asyncRabbitTemplate must not be null");
        this.properties = Objects.requireNonNull(properties, "properties must not be null");
        this.executorProvider = Objects.requireNonNull(executorProvider, "executorProvider must not be null");
    }

    DefaultReactiveRabbitRpcClient(
            AsyncAmqpTemplate asyncRabbitTemplate,
            RabbitRpcWebFluxBridgeProperties properties,
            Scheduler rpcScheduler
    ) {
        this(asyncRabbitTemplate, properties, new SchedulerBackedRabbitRpcBridgeExecutorProvider(rpcScheduler));
    }

    @Override
    public <T> Mono<T> request(String route, Object request, Class<T> responseType) {
        Objects.requireNonNull(responseType, "responseType must not be null");
        return request(route, request, ParameterizedTypeReference.forType(responseType), RpcOptions.of(properties.getResponseMode()));
    }

    @Override
    public <T> Mono<T> request(String route, Object request, Class<T> responseType, RpcOptions options) {
        Objects.requireNonNull(responseType, "responseType must not be null");
        return request(route, request, ParameterizedTypeReference.forType(responseType), options);
    }

    @Override
    public <T> Mono<T> request(
            String route,
            Object request,
            ParameterizedTypeReference<T> responseType,
            RpcOptions options
    ) {
        if (route == null || route.isBlank()) {
            throw new IllegalArgumentException("route must not be blank");
        }
        Objects.requireNonNull(responseType, "responseType must not be null");
        RpcOptions effectiveOptions = options == null ? RpcOptions.of(properties.getResponseMode()) : options;
        Duration timeout = effectiveOptions.timeoutOr(properties.getDefaultTimeout());
        RpcTimeoutPolicy timeoutPolicy = new RpcTimeoutPolicy(timeout);

        return Mono.deferContextual(contextView -> {
            RpcContext rpcContext = ReactiveRpcContext.current(contextView).orElse(RpcContext.empty());
            Map<String, String> headers = headers(rpcContext);
            String correlationId = correlationId(headers);
            headers.putIfAbsent(RpcHeaders.CORRELATION_ID, correlationId);
            MessagePostProcessor postProcessor = message -> withRpcHeaders(message, correlationId, headers);

            Mono<T> result = switch (effectiveOptions.responseMode()) {
                case RAW -> rawRequest(route, request, responseType, postProcessor);
                case ENVELOPE -> envelopeRequest(route, request, responseType, postProcessor);
            };

            // Reactor timeout cancels the local future, but RabbitMQ may still process a request already accepted by the broker.
            return result.timeout(timeoutPolicy.requestTimeout());
        });
    }

    private <T> Mono<T> rawRequest(
            String route,
            Object request,
            ParameterizedTypeReference<T> responseType,
            MessagePostProcessor postProcessor
    ) {
        return executorProvider.execute(() -> asyncRabbitTemplate.convertSendAndReceiveAsType(
                properties.getExchange(),
                route,
                request,
                postProcessor,
                responseType
        ));
    }

    private <T> Mono<T> envelopeRequest(
            String route,
            Object request,
            ParameterizedTypeReference<T> responseType,
            MessagePostProcessor postProcessor
    ) {
        ParameterizedTypeReference<RpcResponseEnvelope<T>> envelopeType = envelopeType(responseType);
        return executorProvider.execute(() -> envelopeFuture(route, request, postProcessor, envelopeType))
                .flatMap(this::unwrapEnvelope);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private <T> CompletableFuture<Object> envelopeFuture(
            String route,
            Object request,
            MessagePostProcessor postProcessor,
            ParameterizedTypeReference<RpcResponseEnvelope<T>> envelopeType
    ) {
        CompletableFuture future = asyncRabbitTemplate.convertSendAndReceiveAsType(
                properties.getExchange(),
                route,
                request,
                postProcessor,
                envelopeType
        );
        // Phase 14.10.1 maps only the explicit application-level envelope protocol.
        // Transport, future, and conversion failures still propagate as-is.
        return future;
    }

    @SuppressWarnings("unchecked")
    private <T> ParameterizedTypeReference<RpcResponseEnvelope<T>> envelopeType(ParameterizedTypeReference<T> responseType) {
        ResolvableType payloadType = ResolvableType.forType(responseType.getType());
        ResolvableType envelopeType = ResolvableType.forClassWithGenerics(RpcResponseEnvelope.class, payloadType);
        return (ParameterizedTypeReference<RpcResponseEnvelope<T>>) (Object) ParameterizedTypeReference.forType(envelopeType.getType());
    }

    @SuppressWarnings("unchecked")
    private <T> Mono<T> unwrapEnvelope(Object reply) {
        if (!(reply instanceof RpcResponseEnvelope<?> envelope)) {
            return Mono.error(new IllegalStateException("RPC envelope response is malformed"));
        }
        if (envelope.getStatus() == RpcResponseEnvelope.RpcEnvelopeStatus.SUCCESS) {
            return Mono.justOrEmpty((T) envelope.getPayload());
        }
        if (envelope.getStatus() == RpcResponseEnvelope.RpcEnvelopeStatus.ERROR) {
            return Mono.error(new RabbitRpcRemoteException(
                    envelope.getErrorCode(),
                    envelope.getErrorMessage(),
                    envelope.getErrorType()
            ));
        }
        return Mono.error(new IllegalStateException("RPC envelope response is malformed"));
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

    private record SchedulerBackedRabbitRpcBridgeExecutorProvider(Scheduler scheduler) implements RabbitRpcBridgeExecutorProvider {
        private SchedulerBackedRabbitRpcBridgeExecutorProvider {
            Objects.requireNonNull(scheduler, "scheduler must not be null");
        }

        @Override
        public void close() {
        }
    }
}
