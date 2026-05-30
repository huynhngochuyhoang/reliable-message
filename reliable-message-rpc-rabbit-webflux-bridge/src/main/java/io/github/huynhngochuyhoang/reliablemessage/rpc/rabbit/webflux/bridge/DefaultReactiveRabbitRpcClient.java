package io.github.huynhngochuyhoang.reliablemessage.rpc.rabbit.webflux.bridge;

import io.github.huynhngochuyhoang.reliablemessage.rpc.RpcContext;
import io.github.huynhngochuyhoang.reliablemessage.rpc.RpcExceptionClassifier;
import io.github.huynhngochuyhoang.reliablemessage.rpc.RpcHeaders;
import io.github.huynhngochuyhoang.reliablemessage.rpc.RpcTimeoutPolicy;
import io.github.huynhngochuyhoang.reliablemessage.rpc.webflux.ReactiveRpcContext;
import io.micrometer.core.instrument.Timer;
import org.springframework.amqp.core.AsyncAmqpTemplate;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.rabbit.AsyncRabbitTemplate;
import org.springframework.amqp.support.converter.MessageConversionException;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.core.ResolvableType;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;

public class DefaultReactiveRabbitRpcClient implements ReactiveRabbitRpcClient {

    private final AsyncAmqpTemplate asyncRabbitTemplate;
    private final RabbitRpcWebFluxBridgeProperties properties;
    private final RabbitRpcBridgeExecutorProvider executorProvider;
    private final RabbitRpcMetrics metrics;
    private final RpcExceptionClassifier exceptionClassifier = RpcExceptionClassifier.defaults();

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
        this(asyncRabbitTemplate, properties, executorProvider, RabbitRpcMetrics.noop(
                properties == null ? null : properties.getExecutorMode()
        ));
    }

    public DefaultReactiveRabbitRpcClient(
            AsyncAmqpTemplate asyncRabbitTemplate,
            RabbitRpcWebFluxBridgeProperties properties,
            RabbitRpcBridgeExecutorProvider executorProvider,
            RabbitRpcMetrics metrics
    ) {
        this.asyncRabbitTemplate = Objects.requireNonNull(asyncRabbitTemplate, "asyncRabbitTemplate must not be null");
        this.properties = Objects.requireNonNull(properties, "properties must not be null");
        this.executorProvider = Objects.requireNonNull(executorProvider, "executorProvider must not be null");
        this.metrics = Objects.requireNonNull(metrics, "metrics must not be null");
    }

    DefaultReactiveRabbitRpcClient(
            AsyncAmqpTemplate asyncRabbitTemplate,
            RabbitRpcWebFluxBridgeProperties properties,
            Scheduler rpcScheduler
    ) {
        this(asyncRabbitTemplate, properties, new SchedulerBackedRabbitRpcBridgeExecutorProvider(rpcScheduler));
    }

    DefaultReactiveRabbitRpcClient(
            AsyncAmqpTemplate asyncRabbitTemplate,
            RabbitRpcWebFluxBridgeProperties properties,
            Scheduler rpcScheduler,
            RabbitRpcMetrics metrics
    ) {
        this(asyncRabbitTemplate, properties, new SchedulerBackedRabbitRpcBridgeExecutorProvider(rpcScheduler), metrics);
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
            Timer.Sample sample = metricsSample();
            recordMetrics(() -> metrics.request(route));
            RpcContext rpcContext = ReactiveRpcContext.current(contextView).orElse(RpcContext.empty());
            Map<String, String> headers = headers(rpcContext);
            String correlationId = correlationId(headers);
            headers.putIfAbsent(RpcHeaders.CORRELATION_ID, correlationId);
            MessagePostProcessor postProcessor = message -> withRpcHeaders(message, correlationId, headers);

            Mono<T> attempt = Mono.defer(() -> switch (effectiveOptions.responseMode()) {
                case RAW -> rawRequest(route, request, responseType, postProcessor);
                case ENVELOPE -> envelopeRequest(route, request, responseType, postProcessor);
            }).timeout(timeoutPolicy.requestTimeout());

            return retry(route, attempt, 1)
                    .doOnSuccess(value -> {
                        recordMetrics(() -> metrics.success(route));
                        recordMetrics(() -> metrics.duration(sample, route, "success"));
                    })
                    .doOnError(error -> {
                        String status = status(error);
                        recordMetrics(() -> metrics.failure(route, status));
                        if (exceptionClassifier.timeout(error)) {
                            recordMetrics(() -> metrics.timeout(route));
                        }
                        if (error instanceof RabbitRpcBridgeRejectedException) {
                            recordMetrics(() -> metrics.bulkheadRejected(route));
                        }
                        recordMetrics(() -> metrics.duration(sample, route, status));
                    });
        });
    }

    private <T> Mono<T> retry(String route, Mono<T> source, int attempt) {
        return source.onErrorResume(error -> {
            if (attempt >= properties.getMaxAttempts() || !retryable(error)) {
                return Mono.error(error);
            }
            recordMetrics(() -> metrics.retry(route));
            Duration backoff = retryBackoff(attempt);
            Mono<Void> delay = backoff.isZero() ? Mono.empty() : Mono.delay(backoff).then();
            return delay.then(retry(route, source, attempt + 1));
        });
    }

    private boolean retryable(Throwable error) {
        return !conversionFailure(error) && exceptionClassifier.retryable(error);
    }

    private static boolean conversionFailure(Throwable error) {
        Throwable current = error;
        while (current != null) {
            if (current instanceof MessageConversionException) {
                return true;
            }
            Throwable cause = current.getCause();
            current = cause == current ? null : cause;
        }
        return false;
    }

    private Timer.Sample metricsSample() {
        try {
            return metrics.start();
        } catch (RuntimeException ignored) {
            return null;
        }
    }

    private static void recordMetrics(Runnable recorder) {
        try {
            recorder.run();
        } catch (RuntimeException ignored) {
            // Metrics must never alter RPC request/response behavior.
        }
    }

    private Duration retryBackoff(int attempt) {
        List<Duration> backoff = properties.getRetryBackoff();
        int index = Math.min(attempt - 1, backoff.size() - 1);
        return backoff.get(index);
    }

    private String status(Throwable error) {
        if (error instanceof RabbitRpcBridgeRejectedException) {
            return "bulkhead_rejected";
        }
        if (exceptionClassifier.timeout(error)) {
            return "timeout";
        }
        if (error instanceof RabbitRpcRemoteException) {
            return "remote_error";
        }
        return "failure";
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

    @SuppressWarnings({"unchecked", "rawtypes"})
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
