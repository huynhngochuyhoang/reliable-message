package io.github.huynhngochuyhoang.reliablemessage.rpc.rabbit.webflux.bridge;

import io.github.huynhngochuyhoang.reliablemessage.rpc.RpcContext;
import io.github.huynhngochuyhoang.reliablemessage.rpc.RpcExceptionClassifier;
import io.github.huynhngochuyhoang.reliablemessage.rpc.RpcHeaders;
import io.github.huynhngochuyhoang.reliablemessage.rpc.webflux.ReactiveRpcContext;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.AsyncAmqpTemplate;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.core.ParameterizedTypeReference;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DefaultReactiveRabbitRpcClientTest {

    @Test
    void requestSendsThroughAsyncRabbitTemplate() throws Exception {
        RecordingAsyncAmqpTemplate template = new RecordingAsyncAmqpTemplate();
        template.nextFuture.complete("accepted");
        DefaultReactiveRabbitRpcClient client = client(template, Duration.ofSeconds(5), "rpc.exchange");

        StepVerifier.create(client.request("orders.lookup", "request", String.class))
                .expectNext("accepted")
                .verifyComplete();

        assertThat(template.exchange).isEqualTo("rpc.exchange");
        assertThat(template.routingKey).isEqualTo("orders.lookup");
        assertThat(template.request).isEqualTo("request");
        assertThat(template.responseType.getType()).isEqualTo(String.class);
    }

    @Test
    void parameterizedResponseTypeIsSentThroughAsyncRabbitTemplate() {
        RecordingAsyncAmqpTemplate template = new RecordingAsyncAmqpTemplate();
        OrderResponse response = new OrderResponse("order-1");
        template.nextFuture.complete(List.of(response));
        DefaultReactiveRabbitRpcClient client = client(template, Duration.ofSeconds(5), "");
        ParameterizedTypeReference<List<OrderResponse>> responseType = new ParameterizedTypeReference<>() {
        };

        StepVerifier.create(client.request("orders.lookup", "request", responseType, RpcOptions.raw()))
                .expectNext(List.of(response))
                .verifyComplete();

        assertThat(template.responseType.getType()).isEqualTo(responseType.getType());
    }

    @Test
    void customClientCanImplementOnlyClassOverload() {
        ReactiveRabbitRpcClient client = new ClassOnlyReactiveRabbitRpcClient();

        StepVerifier.create(client.request("orders.lookup", "request", String.class, RpcOptions.raw()))
                .expectNext("legacy")
                .verifyComplete();
    }

    @Test
    void successEnvelopeReturnsPayload() {
        RecordingAsyncAmqpTemplate template = new RecordingAsyncAmqpTemplate();
        OrderResponse response = new OrderResponse("order-1");
        template.nextFuture.complete(RpcResponseEnvelope.success(response));
        DefaultReactiveRabbitRpcClient client = client(template, Duration.ofSeconds(5), "");

        StepVerifier.create(client.request("orders.lookup", "request", OrderResponse.class, RpcOptions.envelope()))
                .expectNext(response)
                .verifyComplete();
    }

    @Test
    void errorEnvelopeMapsToRemoteException() {
        RecordingAsyncAmqpTemplate template = new RecordingAsyncAmqpTemplate();
        template.nextFuture.complete(RpcResponseEnvelope.error("ORDER_NOT_FOUND", "Order missing", "NotFound"));
        DefaultReactiveRabbitRpcClient client = client(template, Duration.ofSeconds(5), "");

        StepVerifier.create(client.request("orders.lookup", "request", OrderResponse.class, RpcOptions.envelope()))
                .expectErrorSatisfies(error -> assertThat(error)
                        .isInstanceOf(RabbitRpcRemoteException.class)
                        .satisfies(remote -> {
                            RabbitRpcRemoteException exception = (RabbitRpcRemoteException) remote;
                            assertThat(exception.getErrorCode()).isEqualTo("ORDER_NOT_FOUND");
                            assertThat(exception.getErrorMessage()).isEqualTo("Order missing");
                            assertThat(exception.getErrorType()).isEqualTo("NotFound");
                        }))
                .verify();
    }

    @Test
    void malformedEnvelopePropagatesConversionError() {
        RecordingAsyncAmqpTemplate template = new RecordingAsyncAmqpTemplate();
        template.nextFuture.complete("not-an-envelope");
        DefaultReactiveRabbitRpcClient client = client(template, Duration.ofSeconds(5), "");

        StepVerifier.create(client.request("orders.lookup", "request", OrderResponse.class, RpcOptions.envelope()))
                .expectErrorSatisfies(error -> assertThat(error)
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessageContaining("RPC envelope"))
                .verify();
    }

    @Test
    void inFlightRpcFutureCompletesReturnedMonoSuccessfully() {
        RecordingAsyncAmqpTemplate template = new RecordingAsyncAmqpTemplate();
        DefaultReactiveRabbitRpcClient client = client(template, Duration.ofSeconds(5), "");

        StepVerifier.create(client.request("orders.lookup", "request", String.class))
                .then(() -> {
                    assertThat(template.invoked).isTrue();
                    assertThat(template.nextFuture.isDone()).isFalse();
                })
                .then(() -> template.nextFuture.complete("reply"))
                .expectNext("reply")
                .verifyComplete();
    }

    @Test
    void requestMonoIsLazyBeforeSubscription() {
        RecordingAsyncAmqpTemplate template = new RecordingAsyncAmqpTemplate();
        DefaultReactiveRabbitRpcClient client = client(template, Duration.ofSeconds(5), "");

        Mono<String> result = client.request("orders.lookup", "request", String.class);

        assertThat(template.invoked).isFalse();
        StepVerifier.create(result)
                .then(() -> template.nextFuture.complete("reply"))
                .expectNext("reply")
                .verifyComplete();
        assertThat(template.invoked).isTrue();
    }

    @Test
    void requestCreationRunsOnRpcBridgeExecutorThread() throws Exception {
        RecordingAsyncAmqpTemplate template = new RecordingAsyncAmqpTemplate();
        Scheduler scheduler = Schedulers.newSingle("rabbit-rpc-bridge-test");
        try {
            DefaultReactiveRabbitRpcClient client = client(template, Duration.ofSeconds(5), "", scheduler);
            String callerThread = Thread.currentThread().getName();

            StepVerifier.create(client.request("orders.lookup", "request", String.class))
                    .then(() -> assertThat(template.awaitInvocation()).isTrue())
                    .then(() -> template.nextFuture.complete("reply"))
                    .expectNext("reply")
                    .verifyComplete();

            assertThat(template.invocationThread).startsWith("rabbit-rpc-bridge-test");
            assertThat(template.invocationThread).isNotEqualTo(callerThread);
        } finally {
            scheduler.dispose();
        }
    }

    @Test
    void virtualThreadExecutorOffloadsWithRpcThreadName() {
        RecordingAsyncAmqpTemplate template = new RecordingAsyncAmqpTemplate();
        RabbitRpcWebFluxBridgeProperties properties = new RabbitRpcWebFluxBridgeProperties();
        properties.setExecutorMode(RabbitRpcExecutorMode.VIRTUAL_THREAD);
        properties.setMaxConcurrency(1);

        try (RabbitRpcBridgeExecutorProvider provider = RabbitRpcBridgeExecutorProvider.create(properties)) {
            DefaultReactiveRabbitRpcClient client = new DefaultReactiveRabbitRpcClient(template, properties, provider);

            StepVerifier.create(client.request("orders.lookup", "request", String.class))
                    .then(() -> assertThat(template.awaitInvocation()).isTrue())
                    .then(() -> template.nextFuture.complete("reply"))
                    .expectNext("reply")
                    .verifyComplete();

            assertThat(template.invocationThread).startsWith("rabbit-rpc-bridge-virtual-");
        }
    }

    @Test
    void timeoutFailsReturnedMonoWithTimeoutClassifiedError() {
        RecordingAsyncAmqpTemplate template = new RecordingAsyncAmqpTemplate();
        DefaultReactiveRabbitRpcClient client = client(template, Duration.ofMillis(10), "");

        StepVerifier.withVirtualTime(() -> client.request("orders.lookup", "request", String.class))
                .thenAwait(Duration.ofMillis(10))
                .expectErrorSatisfies(error -> assertThat(RpcExceptionClassifier.defaults().timeout(error)).isTrue())
                .verify();

        awaitCondition(template.nextFuture::isCancelled, "RPC future cancellation");
    }

    @Test
    void cancellationCancelsUnderlyingFuture() throws Exception {
        RecordingAsyncAmqpTemplate template = new RecordingAsyncAmqpTemplate();
        Scheduler scheduler = Schedulers.newSingle("rabbit-rpc-bridge-test");
        try {
            DefaultReactiveRabbitRpcClient client = client(template, Duration.ofSeconds(5), "", scheduler);

            Disposable subscription = client.request("orders.lookup", "request", String.class).subscribe();
            assertThat(template.awaitInvocation()).isTrue();
            subscription.dispose();

            awaitCondition(template.nextFuture::isCancelled, "RPC future cancellation");
        } finally {
            scheduler.dispose();
        }
    }

    @Test
    void maxConcurrencyRejectsWhenSaturated() {
        RecordingAsyncAmqpTemplate template = new RecordingAsyncAmqpTemplate();
        RabbitRpcWebFluxBridgeProperties properties = new RabbitRpcWebFluxBridgeProperties();
        properties.setMaxConcurrency(1);

        try (RabbitRpcBridgeExecutorProvider provider = RabbitRpcBridgeExecutorProvider.create(properties)) {
            DefaultReactiveRabbitRpcClient client = new DefaultReactiveRabbitRpcClient(template, properties, provider);
            Disposable firstSubscription = client.request("orders.lookup", "request", String.class).subscribe();
            assertThat(template.awaitInvocation()).isTrue();

            StepVerifier.create(client.request("orders.lookup", "request", String.class))
                    .expectError(RabbitRpcBridgeRejectedException.class)
                    .verify();

            firstSubscription.dispose();
        }
    }

    @Test
    void maxConcurrencyPermitReleasesAfterSuccessFailureTimeoutCancellationAndExecutorRejection() {
        RabbitRpcWebFluxBridgeProperties properties = new RabbitRpcWebFluxBridgeProperties();
        properties.setMaxConcurrency(1);

        try (RabbitRpcBridgeExecutorProvider provider = RabbitRpcBridgeExecutorProvider.create(properties)) {
            RecordingAsyncAmqpTemplate successTemplate = new RecordingAsyncAmqpTemplate();
            successTemplate.nextFuture.complete("ok");
            StepVerifier.create(new DefaultReactiveRabbitRpcClient(successTemplate, properties, provider)
                            .request("orders.lookup", "request", String.class))
                    .expectNext("ok")
                    .verifyComplete();

            RecordingAsyncAmqpTemplate failureTemplate = new RecordingAsyncAmqpTemplate();
            RuntimeException failure = new RuntimeException("broker failure");
            failureTemplate.nextFuture.completeExceptionally(failure);
            StepVerifier.create(new DefaultReactiveRabbitRpcClient(failureTemplate, properties, provider)
                            .request("orders.lookup", "request", String.class))
                    .expectErrorMatches(error -> error == failure)
                    .verify();

            RecordingAsyncAmqpTemplate timeoutTemplate = new RecordingAsyncAmqpTemplate();
            RabbitRpcWebFluxBridgeProperties timeoutProperties = new RabbitRpcWebFluxBridgeProperties();
            timeoutProperties.setDefaultTimeout(Duration.ofMillis(50));
            timeoutProperties.setMaxConcurrency(1);
            StepVerifier.create(new DefaultReactiveRabbitRpcClient(timeoutTemplate, timeoutProperties, provider)
                            .request("orders.lookup", "request", String.class))
                    .then(() -> assertThat(timeoutTemplate.awaitInvocation()).isTrue())
                    .expectErrorSatisfies(error -> assertThat(RpcExceptionClassifier.defaults().timeout(error)).isTrue())
                    .verify();

            awaitCondition(timeoutTemplate.nextFuture::isCancelled, "RPC future cancellation after timeout");

            RecordingAsyncAmqpTemplate cancellationTemplate = new RecordingAsyncAmqpTemplate();
            Disposable subscription = new DefaultReactiveRabbitRpcClient(cancellationTemplate, properties, provider)
                    .request("orders.lookup", "request", String.class)
                    .subscribe();
            assertThat(cancellationTemplate.awaitInvocation()).isTrue();
            subscription.dispose();

            awaitCondition(cancellationTemplate.nextFuture::isCancelled, "RPC future cancellation");

            RecordingAsyncAmqpTemplate afterCancellationTemplate = new RecordingAsyncAmqpTemplate();
            afterCancellationTemplate.nextFuture.complete("after-cancel");
            StepVerifier.create(new DefaultReactiveRabbitRpcClient(afterCancellationTemplate, properties, provider)
                            .request("orders.lookup", "request", String.class))
                    .expectNext("after-cancel")
                    .verifyComplete();
        }
        RabbitRpcBridgeExecutorProvider closedProvider = RabbitRpcBridgeExecutorProvider.create(properties);
        closedProvider.close();
        RecordingAsyncAmqpTemplate rejectedTemplate = new RecordingAsyncAmqpTemplate();
        StepVerifier.create(new DefaultReactiveRabbitRpcClient(rejectedTemplate, properties, closedProvider)
                        .request("orders.lookup", "request", String.class))
                .expectError(RabbitRpcBridgeRejectedException.class)
                .verify();

        RecordingAsyncAmqpTemplate afterRejectionTemplate = new RecordingAsyncAmqpTemplate();
        afterRejectionTemplate.nextFuture.complete("after-rejection");
        try (RabbitRpcBridgeExecutorProvider provider = RabbitRpcBridgeExecutorProvider.create(properties)) {
            StepVerifier.create(new DefaultReactiveRabbitRpcClient(afterRejectionTemplate, properties, provider)
                            .request("orders.lookup", "request", String.class))
                    .expectNext("after-rejection")
                    .verifyComplete();
        }
    }

    @Test
    void brokerRequestFailurePropagates() {
        RecordingAsyncAmqpTemplate template = new RecordingAsyncAmqpTemplate();
        template.failureOnSend = new AmqpException("broker down");
        DefaultReactiveRabbitRpcClient client = client(template, Duration.ofSeconds(5), "");

        StepVerifier.create(client.request("orders.lookup", "request", String.class))
                .expectErrorSatisfies(error -> assertThat(error)
                        .isInstanceOf(AmqpException.class)
                        .hasMessageContaining("broker down"))
                .verify();
    }

    @Test
    void remoteFailurePropagates() {
        RecordingAsyncAmqpTemplate template = new RecordingAsyncAmqpTemplate();
        RuntimeException remoteFailure = new RuntimeException("remote rejected request");
        template.nextFuture.completeExceptionally(remoteFailure);
        DefaultReactiveRabbitRpcClient client = client(template, Duration.ofSeconds(5), "");

        StepVerifier.create(client.request("orders.lookup", "request", String.class))
                .expectErrorSatisfies(error -> assertThat(error).isSameAs(remoteFailure))
                .verify();
    }

    @Test
    void replyDeserializationFailurePropagates() {
        RecordingAsyncAmqpTemplate template = new RecordingAsyncAmqpTemplate();
        IllegalArgumentException conversionFailure = new IllegalArgumentException("cannot convert reply");
        template.nextFuture.completeExceptionally(conversionFailure);
        DefaultReactiveRabbitRpcClient client = client(template, Duration.ofSeconds(5), "");

        StepVerifier.create(client.request("orders.lookup", "request", String.class))
                .expectErrorSatisfies(error -> assertThat(error).isSameAs(conversionFailure))
                .verify();
    }

    @Test
    void correlationIdAndRpcHeadersArePropagatedFromReactorContext() throws Exception {
        RecordingAsyncAmqpTemplate template = new RecordingAsyncAmqpTemplate();
        template.nextFuture.complete("reply");
        DefaultReactiveRabbitRpcClient client = client(template, Duration.ofSeconds(5), "");
        RpcContext rpcContext = RpcContext.builder()
                .correlationId("correlation-1")
                .requestId("request-1")
                .traceId("trace-1")
                .tenantId("tenant-1")
                .header("x-custom", "custom-value")
                .build();

        StepVerifier.create(client.request("orders.lookup", "request", String.class)
                        .contextWrite(ReactiveRpcContext.write(rpcContext)))
                .expectNext("reply")
                .verifyComplete();

        Message processed = template.postProcessor.postProcessMessage(new Message(new byte[0], new MessageProperties()));
        assertThat(processed.getMessageProperties().getCorrelationId()).isEqualTo("correlation-1");
        assertThat((Object) processed.getMessageProperties().getHeader(RpcHeaders.CORRELATION_ID)).isEqualTo("correlation-1");
        assertThat((Object) processed.getMessageProperties().getHeader(RpcHeaders.REQUEST_ID)).isEqualTo("request-1");
        assertThat((Object) processed.getMessageProperties().getHeader(RpcHeaders.TRACE_ID)).isEqualTo("trace-1");
        assertThat((Object) processed.getMessageProperties().getHeader(RpcHeaders.TENANT_ID)).isEqualTo("tenant-1");
        assertThat((Object) processed.getMessageProperties().getHeader("x-custom")).isEqualTo("custom-value");
    }

    @Test
    void generatedCorrelationIdIsPropagatedWhenContextHasNone() throws Exception {
        RecordingAsyncAmqpTemplate template = new RecordingAsyncAmqpTemplate();
        template.nextFuture.complete("reply");
        DefaultReactiveRabbitRpcClient client = client(template, Duration.ofSeconds(5), "");

        StepVerifier.create(client.request("orders.lookup", "request", String.class))
                .expectNext("reply")
                .verifyComplete();

        Message processed = template.postProcessor.postProcessMessage(new Message(new byte[0], new MessageProperties()));
        String correlationId = processed.getMessageProperties().getCorrelationId();
        assertThat(correlationId).isNotBlank();
        assertThat((Object) processed.getMessageProperties().getHeader(RpcHeaders.CORRELATION_ID)).isEqualTo(correlationId);
    }

    @Test
    void reactorContextIsPreservedAroundRpcMono() {
        RecordingAsyncAmqpTemplate template = new RecordingAsyncAmqpTemplate();
        template.nextFuture.complete("reply");
        DefaultReactiveRabbitRpcClient client = client(template, Duration.ofSeconds(5), "");

        StepVerifier.create(client.request("orders.lookup", "request", String.class)
                        .flatMap(reply -> Mono.deferContextual(context -> Mono.just(reply + ":" + context.get("marker"))))
                        .contextWrite(context -> context.put("marker", "kept")))
                .expectNext("reply:kept")
                .verifyComplete();
    }

    @Test
    void defaultTimeoutIsNonNullAndPositive() {
        RabbitRpcWebFluxBridgeProperties properties = new RabbitRpcWebFluxBridgeProperties();

        assertThat(properties.getDefaultTimeout()).isNotNull();
        assertThat(properties.getDefaultTimeout()).isPositive();
    }

    @Test
    void zeroOrNegativeTimeoutFailsValidation() {
        RabbitRpcWebFluxBridgeProperties properties = new RabbitRpcWebFluxBridgeProperties();

        assertThatThrownBy(() -> properties.setDefaultTimeout(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("defaultTimeout must be positive");
        assertThatThrownBy(() -> properties.setDefaultTimeout(Duration.ofMillis(-1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("defaultTimeout must be positive");
    }

    @Test
    void invalidExecutorPropertiesFailValidation() {
        RabbitRpcWebFluxBridgeProperties properties = new RabbitRpcWebFluxBridgeProperties();

        assertThatThrownBy(() -> properties.setExecutorMode(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("executorMode must not be null");
        assertThatThrownBy(() -> properties.setMaxConcurrency(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxConcurrency must be positive");
    }

    private static void awaitCondition(BooleanSupplier condition, String description) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        assertThat(condition.getAsBoolean()).as(description).isTrue();
    }

    private static DefaultReactiveRabbitRpcClient client(
            RecordingAsyncAmqpTemplate template,
            Duration timeout,
            String exchange
    ) {
        return client(template, timeout, exchange, Schedulers.immediate());
    }

    private static DefaultReactiveRabbitRpcClient client(
            RecordingAsyncAmqpTemplate template,
            Duration timeout,
            String exchange,
            Scheduler scheduler
    ) {
        RabbitRpcWebFluxBridgeProperties properties = new RabbitRpcWebFluxBridgeProperties();
        properties.setDefaultTimeout(timeout);
        properties.setExchange(exchange);
        return new DefaultReactiveRabbitRpcClient(template, properties, scheduler);
    }

    private static final class RecordingAsyncAmqpTemplate implements AsyncAmqpTemplate {
        private final CompletableFuture<Object> nextFuture = new CompletableFuture<>();
        private RuntimeException failureOnSend;
        private String exchange;
        private String routingKey;
        private Object request;
        private MessagePostProcessor postProcessor;
        private ParameterizedTypeReference<?> responseType;
        private String invocationThread;
        private boolean invoked;
        private final CountDownLatch invocationLatch = new CountDownLatch(1);

        @Override
        @SuppressWarnings("unchecked")
        public <C> CompletableFuture<C> convertSendAndReceiveAsType(
                String exchange,
                String routingKey,
                Object message,
                MessagePostProcessor messagePostProcessor,
                ParameterizedTypeReference<C> responseType
        ) {
            if (failureOnSend != null) {
                throw failureOnSend;
            }
            this.exchange = exchange;
            this.routingKey = routingKey;
            this.request = message;
            this.postProcessor = messagePostProcessor;
            this.responseType = responseType;
            this.invocationThread = Thread.currentThread().getName();
            this.invoked = true;
            this.invocationLatch.countDown();
            return (CompletableFuture<C>) nextFuture;
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

    private static final class ClassOnlyReactiveRabbitRpcClient implements ReactiveRabbitRpcClient {

        @Override
        @SuppressWarnings("unchecked")
        public <T> Mono<T> request(String route, Object request, Class<T> responseType) {
            return Mono.just((T) "legacy");
        }
    }

    private record OrderResponse(String id) {
    }
}
