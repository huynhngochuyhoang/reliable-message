package io.github.huynhngochuyhoang.reliablemessage.rpc.rabbit.webflux.bridge;

import io.github.huynhngochuyhoang.reliablemessage.rpc.RpcContext;
import io.github.huynhngochuyhoang.reliablemessage.rpc.RpcExceptionClassifier;
import io.github.huynhngochuyhoang.reliablemessage.rpc.RpcHeaders;
import io.github.huynhngochuyhoang.reliablemessage.rpc.webflux.ReactiveRpcContext;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.AsyncAmqpTemplate;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.support.converter.MessageConversionException;
import org.springframework.core.ParameterizedTypeReference;
import reactor.core.Disposable;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class RabbitRpcResilienceTest {

    @Test
    void retriesRetryableFutureFailureAndStopsAfterMaxAttempts() {
        RecordingAsyncAmqpTemplate template = new RecordingAsyncAmqpTemplate();
        CompletableFuture<Object> first = new CompletableFuture<>();
        first.completeExceptionally(new IOException("connection reset"));
        template.addFuture(first);
        template.addFuture(CompletableFuture.completedFuture("reply"));
        RabbitRpcWebFluxBridgeProperties properties = retryProperties(2, Duration.ZERO);
        DefaultReactiveRabbitRpcClient client = new DefaultReactiveRabbitRpcClient(
                template, properties, new SchedulerBackedProvider(Schedulers.immediate()));

        StepVerifier.create(client.request("orders.lookup", "request", String.class))
                .expectNext("reply")
                .verifyComplete();

        assertThat(template.invocationCount).isEqualTo(2);
    }

    @Test
    void retriesRetryableTimeout() {
        RecordingAsyncAmqpTemplate template = new RecordingAsyncAmqpTemplate();
        template.addFuture(new CompletableFuture<>());
        template.addFuture(CompletableFuture.completedFuture("reply"));
        RabbitRpcWebFluxBridgeProperties properties = retryProperties(2, Duration.ZERO);
        properties.setDefaultTimeout(Duration.ofMillis(10));
        DefaultReactiveRabbitRpcClient client = new DefaultReactiveRabbitRpcClient(
                template, properties, new SchedulerBackedProvider(Schedulers.immediate()));

        StepVerifier.withVirtualTime(() -> client.request("orders.lookup", "request", String.class))
                .thenAwait(Duration.ofMillis(10))
                .expectNext("reply")
                .verifyComplete();

        assertThat(template.invocationCount).isEqualTo(2);
    }

    @Test
    void retryBackoffIsApplied() {
        RecordingAsyncAmqpTemplate template = new RecordingAsyncAmqpTemplate();
        CompletableFuture<Object> first = new CompletableFuture<>();
        first.completeExceptionally(new IOException("connection reset"));
        template.addFuture(first);
        template.addFuture(CompletableFuture.completedFuture("reply"));
        RabbitRpcWebFluxBridgeProperties properties = retryProperties(2, Duration.ofSeconds(5));
        DefaultReactiveRabbitRpcClient client = new DefaultReactiveRabbitRpcClient(
                template, properties, new SchedulerBackedProvider(Schedulers.immediate()));

        StepVerifier.withVirtualTime(() -> client.request("orders.lookup", "request", String.class))
                .expectSubscription()
                .then(() -> assertThat(template.invocationCount).isEqualTo(1))
                .thenAwait(Duration.ofSeconds(4))
                .then(() -> assertThat(template.invocationCount).isEqualTo(1))
                .thenAwait(Duration.ofSeconds(1))
                .expectNext("reply")
                .verifyComplete();
    }

    @Test
    void doesNotRetryRemoteErrorEnvelopeByDefault() {
        RecordingAsyncAmqpTemplate template = new RecordingAsyncAmqpTemplate();
        template.addFuture(CompletableFuture.completedFuture(RpcResponseEnvelope.error("REMOTE", "failed", "Remote")));
        RabbitRpcWebFluxBridgeProperties properties = retryProperties(3, Duration.ZERO);
        DefaultReactiveRabbitRpcClient client = new DefaultReactiveRabbitRpcClient(
                template, properties, new SchedulerBackedProvider(Schedulers.immediate()));

        StepVerifier.create(client.request("orders.lookup", "request", String.class, RpcOptions.envelope()))
                .expectError(RabbitRpcRemoteException.class)
                .verify();

        assertThat(template.invocationCount).isEqualTo(1);
    }

    @Test
    void doesNotRetryConversionFailure() {
        RecordingAsyncAmqpTemplate template = new RecordingAsyncAmqpTemplate();
        CompletableFuture<Object> failed = new CompletableFuture<>();
        failed.completeExceptionally(new IllegalArgumentException("cannot convert"));
        template.addFuture(failed);
        RabbitRpcWebFluxBridgeProperties properties = retryProperties(3, Duration.ZERO);
        DefaultReactiveRabbitRpcClient client = new DefaultReactiveRabbitRpcClient(
                template, properties, new SchedulerBackedProvider(Schedulers.immediate()));

        StepVerifier.create(client.request("orders.lookup", "request", String.class))
                .expectError(IllegalArgumentException.class)
                .verify();

        assertThat(template.invocationCount).isEqualTo(1);
    }

    @Test
    void doesNotRetryMessageConversionFailureWithIoExceptionRoot() {
        RecordingAsyncAmqpTemplate template = new RecordingAsyncAmqpTemplate();
        CompletableFuture<Object> failed = new CompletableFuture<>();
        failed.completeExceptionally(new MessageConversionException("cannot convert", new IOException("json parser failed")));
        template.addFuture(failed);
        RabbitRpcWebFluxBridgeProperties properties = retryProperties(3, Duration.ZERO);
        DefaultReactiveRabbitRpcClient client = new DefaultReactiveRabbitRpcClient(
                template, properties, new SchedulerBackedProvider(Schedulers.immediate()));

        StepVerifier.create(client.request("orders.lookup", "request", String.class))
                .expectError(MessageConversionException.class)
                .verify();

        assertThat(template.invocationCount).isEqualTo(1);
    }

    @Test
    void metricsFailuresDoNotChangeRpcSignals() {
        RabbitRpcWebFluxBridgeProperties properties = new RabbitRpcWebFluxBridgeProperties();
        ThrowingRabbitRpcMetrics metrics = new ThrowingRabbitRpcMetrics();
        RecordingAsyncAmqpTemplate success = new RecordingAsyncAmqpTemplate();
        success.addFuture(CompletableFuture.completedFuture("reply"));

        StepVerifier.create(new DefaultReactiveRabbitRpcClient(success, properties, new SchedulerBackedProvider(Schedulers.immediate()), metrics)
                        .request("orders.lookup", "request", String.class))
                .expectNext("reply")
                .verifyComplete();

        RecordingAsyncAmqpTemplate failure = new RecordingAsyncAmqpTemplate();
        CompletableFuture<Object> failed = new CompletableFuture<>();
        failed.completeExceptionally(new IllegalArgumentException("cannot convert"));
        failure.addFuture(failed);

        StepVerifier.create(new DefaultReactiveRabbitRpcClient(failure, properties, new SchedulerBackedProvider(Schedulers.immediate()), metrics)
                        .request("orders.lookup", "request", String.class))
                .expectError(IllegalArgumentException.class)
                .verify();
    }

    @Test
    void retryStopsAfterMaxAttempts() {
        RecordingAsyncAmqpTemplate template = new RecordingAsyncAmqpTemplate();
        for (int index = 0; index < 3; index++) {
            CompletableFuture<Object> failed = new CompletableFuture<>();
            failed.completeExceptionally(new IOException("connection reset"));
            template.addFuture(failed);
        }
        RabbitRpcWebFluxBridgeProperties properties = retryProperties(3, Duration.ZERO);
        DefaultReactiveRabbitRpcClient client = new DefaultReactiveRabbitRpcClient(
                template, properties, new SchedulerBackedProvider(Schedulers.immediate()));

        StepVerifier.create(client.request("orders.lookup", "request", String.class))
                .expectError(IOException.class)
                .verify();

        assertThat(template.invocationCount).isEqualTo(3);
    }

    @Test
    void headersArePreservedAndAmqpCorrelationIdIsRegeneratedAcrossRetries() throws Exception {
        RecordingAsyncAmqpTemplate template = new RecordingAsyncAmqpTemplate();
        CompletableFuture<Object> failed = new CompletableFuture<>();
        failed.completeExceptionally(new IOException("connection reset"));
        template.addFuture(failed);
        template.addFuture(CompletableFuture.completedFuture("reply"));
        RabbitRpcWebFluxBridgeProperties properties = retryProperties(2, Duration.ZERO);
        DefaultReactiveRabbitRpcClient client = new DefaultReactiveRabbitRpcClient(
                template, properties, new SchedulerBackedProvider(Schedulers.immediate()));
        RpcContext rpcContext = RpcContext.builder()
                .correlationId("correlation-1")
                .requestId("request-1")
                .build();

        StepVerifier.create(client.request("orders.lookup", "request", String.class)
                        .contextWrite(ReactiveRpcContext.write(rpcContext)))
                .expectNext("reply")
                .verifyComplete();

        assertThat(template.postProcessors).hasSize(2);
        List<Message> processedMessages = template.postProcessors.stream()
                .map(postProcessor -> postProcessor.postProcessMessage(new Message(new byte[0], new MessageProperties())))
                .toList();
        assertThat(processedMessages)
                .extracting(message -> message.getMessageProperties().getCorrelationId())
                .doesNotHaveDuplicates();
        for (Message processed : processedMessages) {
            assertThat((Object) processed.getMessageProperties().getHeader(RpcHeaders.CORRELATION_ID))
                    .isEqualTo("correlation-1");
            assertThat((Object) processed.getMessageProperties().getHeader(RpcHeaders.REQUEST_ID)).isEqualTo("request-1");
        }
    }

    @Test
    void recordsRpcMetricsForSuccessFailureTimeoutRetryAndBulkheadRejection() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RabbitRpcMetrics metrics = new RabbitRpcMetrics(registry, RabbitRpcExecutorMode.PLATFORM);
        RabbitRpcWebFluxBridgeProperties properties = retryProperties(2, Duration.ZERO);
        properties.setDefaultTimeout(Duration.ofMillis(10));

        RecordingAsyncAmqpTemplate success = new RecordingAsyncAmqpTemplate();
        success.addFuture(CompletableFuture.completedFuture("reply"));
        StepVerifier.create(new DefaultReactiveRabbitRpcClient(success, properties, new SchedulerBackedProvider(Schedulers.immediate()), metrics)
                        .request("orders.lookup", "request", String.class))
                .expectNext("reply")
                .verifyComplete();

        RecordingAsyncAmqpTemplate failure = new RecordingAsyncAmqpTemplate();
        CompletableFuture<Object> failed = new CompletableFuture<>();
        failed.completeExceptionally(new IOException("connection reset"));
        failure.addFuture(failed);
        failure.addFuture(CompletableFuture.completedFuture("reply"));
        StepVerifier.create(new DefaultReactiveRabbitRpcClient(failure, properties, new SchedulerBackedProvider(Schedulers.immediate()), metrics)
                        .request("orders.lookup", "request", String.class))
                .expectNext("reply")
                .verifyComplete();

        RecordingAsyncAmqpTemplate timeout = new RecordingAsyncAmqpTemplate();
        timeout.addFuture(new CompletableFuture<>());
        RabbitRpcWebFluxBridgeProperties noRetry = new RabbitRpcWebFluxBridgeProperties();
        noRetry.setDefaultTimeout(Duration.ofMillis(10));
        StepVerifier.withVirtualTime(() -> new DefaultReactiveRabbitRpcClient(timeout, noRetry, new SchedulerBackedProvider(Schedulers.immediate()), metrics)
                        .request("orders.timeout", "request", String.class))
                .thenAwait(Duration.ofMillis(10))
                .expectErrorSatisfies(error -> assertThat(RpcExceptionClassifier.defaults().timeout(error)).isTrue())
                .verify();

        RabbitRpcWebFluxBridgeProperties saturatedProperties = new RabbitRpcWebFluxBridgeProperties();
        saturatedProperties.setMaxConcurrency(1);
        try (RabbitRpcBridgeExecutorProvider provider = RabbitRpcBridgeExecutorProvider.create(saturatedProperties)) {
            RecordingAsyncAmqpTemplate saturated = new RecordingAsyncAmqpTemplate();
            saturated.addFuture(new CompletableFuture<>());
            Disposable first = new DefaultReactiveRabbitRpcClient(saturated, saturatedProperties, provider, metrics)
                    .request("orders.bulkhead", "request", String.class)
                    .subscribe();
            assertThat(saturated.awaitInvocation()).isTrue();

            StepVerifier.create(new DefaultReactiveRabbitRpcClient(saturated, saturatedProperties, provider, metrics)
                            .request("orders.bulkhead", "request", String.class))
                    .expectError(RabbitRpcBridgeRejectedException.class)
                    .verify();
            first.dispose();
        }

        assertThat(counter(registry, "rpc_rabbit_requests_total", "route", "orders.lookup")).isEqualTo(2.0);
        assertThat(counter(registry, "rpc_rabbit_success_total", "route", "orders.lookup")).isEqualTo(2.0);
        assertThat(counter(registry, "rpc_rabbit_retry_total", "route", "orders.lookup")).isEqualTo(1.0);
        assertThat(counter(registry, "rpc_rabbit_timeout_total", "route", "orders.timeout")).isEqualTo(1.0);
        assertThat(counter(registry, "rpc_rabbit_failed_total", "route", "orders.timeout")).isEqualTo(1.0);
        assertThat(counter(registry, "rpc_rabbit_bulkhead_rejected_total", "route", "orders.bulkhead")).isEqualTo(1.0);
        assertThat(registry.find("rpc_rabbit_duration").tag("runtime", "webflux").tag("transport", "rabbit").timers()
                .stream()
                .mapToLong(timer -> timer.count())
                .sum())
                .isGreaterThanOrEqualTo(4);
    }

    private static RabbitRpcWebFluxBridgeProperties retryProperties(int maxAttempts, Duration backoff) {
        RabbitRpcWebFluxBridgeProperties properties = new RabbitRpcWebFluxBridgeProperties();
        properties.setMaxAttempts(maxAttempts);
        properties.setRetryBackoff(List.of(backoff));
        return properties;
    }

    private static double counter(SimpleMeterRegistry registry, String name, String tagName, String tagValue) {
        return registry.find(name).tag(tagName, tagValue).counter().count();
    }

    private record SchedulerBackedProvider(Scheduler scheduler) implements RabbitRpcBridgeExecutorProvider {
        @Override
        public void close() {
        }
    }

    private static final class ThrowingRabbitRpcMetrics extends RabbitRpcMetrics {
        private ThrowingRabbitRpcMetrics() {
            super(null, RabbitRpcExecutorMode.PLATFORM);
        }

        
        public Timer.Sample start() {
            throw new IllegalStateException("metrics failed");
        }

        
        public void request(String route) {
            throw new IllegalStateException("metrics failed");
        }

        
        public void success(String route) {
            throw new IllegalStateException("metrics failed");
        }

        
        public void failure(String route, String status) {
            throw new IllegalStateException("metrics failed");
        }

        
        public void timeout(String route) {
            throw new IllegalStateException("metrics failed");
        }

        
        public void retry(String route) {
            throw new IllegalStateException("metrics failed");
        }

        
        public void bulkheadRejected(String route) {
            throw new IllegalStateException("metrics failed");
        }

        
        public void duration(Timer.Sample sample, String route, String status) {
            throw new IllegalStateException("metrics failed");
        }
    }

    private static final class RecordingAsyncAmqpTemplate implements AsyncAmqpTemplate {
        private final Queue<CompletableFuture<Object>> futures = new ArrayDeque<>();
        private final List<MessagePostProcessor> postProcessors = new ArrayList<>();
        private int invocationCount;
        private final CountDownLatch invocationLatch = new CountDownLatch(1);

        private void addFuture(CompletableFuture<Object> future) {
            futures.add(future);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <C> CompletableFuture<C> convertSendAndReceiveAsType(
                String exchange,
                String routingKey,
                Object message,
                MessagePostProcessor messagePostProcessor,
                ParameterizedTypeReference<C> responseType
        ) {
            this.postProcessors.add(messagePostProcessor);
            this.invocationCount++;
            this.invocationLatch.countDown();
            return (CompletableFuture<C>) futures.remove();
        }

        private boolean awaitInvocation() {
            try {
                return invocationLatch.await(2, TimeUnit.SECONDS);
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
                return false;
            }
        }

        @Override
        public CompletableFuture<Message> sendAndReceive(Message message) {
            throw new UnsupportedOperationException("not used");
        }

        @Override
        public CompletableFuture<Message> sendAndReceive(String routingKey, Message message) {
            throw new UnsupportedOperationException("not used");
        }

        @Override
        public CompletableFuture<Message> sendAndReceive(String exchange, String routingKey, Message message) {
            throw new UnsupportedOperationException("not used");
        }

        @Override
        public <C> CompletableFuture<C> convertSendAndReceive(Object message) {
            throw new UnsupportedOperationException("not used");
        }

        @Override
        public <C> CompletableFuture<C> convertSendAndReceive(String routingKey, Object message) {
            throw new UnsupportedOperationException("not used");
        }

        @Override
        public <C> CompletableFuture<C> convertSendAndReceive(String exchange, String routingKey, Object message) {
            throw new UnsupportedOperationException("not used");
        }

        @Override
        public <C> CompletableFuture<C> convertSendAndReceive(Object message, MessagePostProcessor messagePostProcessor) {
            throw new UnsupportedOperationException("not used");
        }

        @Override
        public <C> CompletableFuture<C> convertSendAndReceive(
                String routingKey,
                Object message,
                MessagePostProcessor messagePostProcessor
        ) {
            throw new UnsupportedOperationException("not used");
        }

        @Override
        public <C> CompletableFuture<C> convertSendAndReceive(
                String exchange,
                String routingKey,
                Object message,
                MessagePostProcessor messagePostProcessor
        ) {
            throw new UnsupportedOperationException("not used");
        }

        @Override
        public <C> CompletableFuture<C> convertSendAndReceiveAsType(
                Object message,
                ParameterizedTypeReference<C> responseType
        ) {
            throw new UnsupportedOperationException("not used");
        }

        @Override
        public <C> CompletableFuture<C> convertSendAndReceiveAsType(
                String routingKey,
                Object message,
                ParameterizedTypeReference<C> responseType
        ) {
            throw new UnsupportedOperationException("not used");
        }

        @Override
        public <C> CompletableFuture<C> convertSendAndReceiveAsType(
                String exchange,
                String routingKey,
                Object message,
                ParameterizedTypeReference<C> responseType
        ) {
            throw new UnsupportedOperationException("not used");
        }

        @Override
        public <C> CompletableFuture<C> convertSendAndReceiveAsType(
                Object message,
                MessagePostProcessor messagePostProcessor,
                ParameterizedTypeReference<C> responseType
        ) {
            throw new UnsupportedOperationException("not used");
        }

        @Override
        public <C> CompletableFuture<C> convertSendAndReceiveAsType(
                String routingKey,
                Object message,
                MessagePostProcessor messagePostProcessor,
                ParameterizedTypeReference<C> responseType
        ) {
            throw new UnsupportedOperationException("not used");
        }
    }
}
