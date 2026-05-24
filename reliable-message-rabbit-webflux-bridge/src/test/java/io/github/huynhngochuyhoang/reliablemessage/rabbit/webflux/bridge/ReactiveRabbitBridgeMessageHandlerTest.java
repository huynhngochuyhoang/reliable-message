package io.github.huynhngochuyhoang.reliablemessage.rabbit.webflux.bridge;

import com.rabbitmq.client.Channel;
import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessage;
import io.github.huynhngochuyhoang.reliablemessage.core.serialization.MessageSerializer;
import io.github.huynhngochuyhoang.reliablemessage.webflux.IdempotencyStartResult;
import io.github.huynhngochuyhoang.reliablemessage.webflux.IdempotencyState;
import io.github.huynhngochuyhoang.reliablemessage.webflux.ReactiveIdempotencyStore;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ReactiveRabbitBridgeMessageHandlerTest {

    @Test
    void reactiveReliableListenerMethodReturningMonoVoidIsInvoked() throws Exception {
        PublicListener listener = new PublicListener(Mono.empty());
        ReactiveRabbitBridgeMessageHandler handler = handler(listener, "handle");
        RecordingChannel channel = new RecordingChannel();

        handler.onMessage(message(), channel.proxy());

        assertThat(listener.invoked()).isTrue();
        assertThat(channel.acked()).isTrue();
    }

    @Test
    void nonPublicListenerMethodCanBeInvoked() throws Exception {
        PrivateListener listener = new PrivateListener();
        ReactiveRabbitBridgeMessageHandler handler = handler(listener, "handle");
        RecordingChannel channel = new RecordingChannel();

        handler.onMessage(message(), channel.proxy());

        assertThat(listener.invoked()).isTrue();
        assertThat(channel.acked()).isTrue();
    }

    @Test
    void handlerMonoIsAwaitedBeforeAck() throws Exception {
        Sinks.Empty<Void> completion = Sinks.empty();
        PublicListener listener = new PublicListener(completion.asMono());
        ReactiveRabbitBridgeMessageHandler handler = handler(listener, "handle");
        RecordingChannel channel = new RecordingChannel();

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> future = executor.submit(() -> invoke(handler, channel));
            await(listener::invoked);

            assertThat(channel.acked()).isFalse();

            completion.tryEmitEmpty();
            future.get(1, TimeUnit.SECONDS);
            assertThat(channel.acked()).isTrue();
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void delayedMonoCompletionDelaysAck() throws Exception {
        PublicListener listener = new PublicListener(Mono.delay(Duration.ofMillis(150)).then());
        ReactiveRabbitBridgeMessageHandler handler = handler(listener, "handle");
        RecordingChannel channel = new RecordingChannel();
        long startedAt = System.nanoTime();

        handler.onMessage(message(), channel.proxy());

        assertThat(channel.acked()).isTrue();
        assertThat(TimeUnit.NANOSECONDS.toMillis(channel.ackTimeNanos() - startedAt)).isGreaterThanOrEqualTo(100);
    }

    @Test
    void handlerErrorDoesNotAckAsSuccess() throws Exception {
        PublicListener listener = new PublicListener(Mono.error(new IllegalStateException("boom")));
        ReactiveRabbitBridgeMessageHandler handler = handler(listener, "handle");
        RecordingChannel channel = new RecordingChannel();

        assertThatThrownBy(() -> handler.onMessage(message(), channel.proxy()))
                .isInstanceOf(IllegalStateException.class);
        assertThat(channel.acked()).isFalse();
        assertThat(channel.nacked()).isTrue();
        assertThat(channel.nackMultiple()).isFalse();
        assertThat(channel.nackRequeue()).isTrue();
    }

    @Test
    void uncheckedNackFailurePreservesOriginalListenerFailure() throws Exception {
        IllegalStateException listenerFailure = new IllegalStateException("listener failed");
        IllegalStateException nackFailure = new IllegalStateException("nack failed");
        PublicListener listener = new PublicListener(Mono.error(listenerFailure));
        ReactiveRabbitBridgeMessageHandler handler = handler(listener, "handle");
        RecordingChannel channel = new RecordingChannel();
        channel.failNackWith(nackFailure);

        assertThatThrownBy(() -> handler.onMessage(message(), channel.proxy()))
                .isSameAs(nackFailure)
                .satisfies(error -> assertThat(error.getSuppressed()).containsExactly(listenerFailure));
        assertThat(channel.acked()).isFalse();
        assertThat(channel.nacked()).isTrue();
    }

    @Test
    void fatalHandlerErrorNacksBeforeRethrow() throws Exception {
        AssertionError fatal = new AssertionError("fatal");
        PublicListener listener = new PublicListener(Mono.error(fatal));
        ReactiveRabbitBridgeMessageHandler handler = handler(listener, "handle");
        RecordingChannel channel = new RecordingChannel();

        assertThatThrownBy(() -> handler.onMessage(message(), channel.proxy()))
                .isSameAs(fatal);
        assertThat(channel.acked()).isFalse();
        assertThat(channel.nacked()).isTrue();
        assertThat(channel.nackMultiple()).isFalse();
        assertThat(channel.nackRequeue()).isTrue();
    }

    @Test
    void newMessageRunsTryStartHandlerMarkSuccessThenAck() throws Exception {
        List<String> events = new ArrayList<>();
        RecordingOrderListener listener = new RecordingOrderListener(events, Mono.empty());
        RecordingIdempotencyStore idempotencyStore = new RecordingIdempotencyStore(
                IdempotencyStartResult.startAccepted(),
                events
        );
        ReactiveRabbitBridgeMessageHandler handler = handler(listener, "handle", idempotencyStore);
        RecordingChannel channel = new RecordingChannel(events);

        handler.onMessage(message(), channel.proxy());

        assertThat(events).containsExactly("tryStart", "handler", "markSuccess", "ack");
        assertThat(idempotencyStore.tryStartKey()).isEqualTo("idempotency-1");
        assertThat(channel.acked()).isTrue();
        assertThat(channel.nacked()).isFalse();
    }

    @Test
    void markSuccessFailureDoesNotAckAsSuccess() throws Exception {
        IllegalStateException markSuccessFailure = new IllegalStateException("mark success failed");
        RecordingIdempotencyStore idempotencyStore = new RecordingIdempotencyStore(
                IdempotencyStartResult.startAccepted(),
                new ArrayList<>()
        );
        idempotencyStore.failMarkSuccessWith(markSuccessFailure);
        PublicListener listener = new PublicListener(Mono.empty());
        ReactiveRabbitBridgeMessageHandler handler = handler(listener, "handle", idempotencyStore);
        RecordingChannel channel = new RecordingChannel();

        assertThatThrownBy(() -> handler.onMessage(message(), channel.proxy()))
                .isSameAs(markSuccessFailure);
        assertThat(listener.invoked()).isTrue();
        assertThat(idempotencyStore.markFailed()).isTrue();
        assertThat(idempotencyStore.markFailedError()).isSameAs(markSuccessFailure);
        assertThat(channel.acked()).isFalse();
        assertThat(channel.nacked()).isTrue();
    }

    @Test
    void duplicateSuccessAcksWithoutInvokingHandler() throws Exception {
        RecordingIdempotencyStore idempotencyStore = new RecordingIdempotencyStore(
                IdempotencyStartResult.duplicate(IdempotencyState.SUCCESS),
                new ArrayList<>()
        );
        PublicListener listener = new PublicListener(Mono.empty());
        ReactiveRabbitBridgeMessageHandler handler = handler(listener, "handle", idempotencyStore);
        RecordingChannel channel = new RecordingChannel();

        handler.onMessage(message(), channel.proxy());

        assertThat(listener.invoked()).isFalse();
        assertThat(idempotencyStore.markSuccess()).isFalse();
        assertThat(channel.acked()).isTrue();
        assertThat(channel.nacked()).isFalse();
    }

    @Test
    void duplicateProcessingDoesNotInvokeHandlerAndDoesNotAckAsSuccess() throws Exception {
        RecordingIdempotencyStore idempotencyStore = new RecordingIdempotencyStore(
                IdempotencyStartResult.duplicate(IdempotencyState.PROCESSING),
                new ArrayList<>()
        );
        PublicListener listener = new PublicListener(Mono.empty());
        ReactiveRabbitBridgeMessageHandler handler = handler(listener, "handle", idempotencyStore);
        RecordingChannel channel = new RecordingChannel();

        assertThatThrownBy(() -> handler.onMessage(message(), channel.proxy()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("PROCESSING");
        assertThat(listener.invoked()).isFalse();
        assertThat(channel.acked()).isFalse();
        assertThat(channel.nacked()).isTrue();
    }

    @Test
    void duplicateFailedDoesNotInvokeHandlerAndDoesNotAckAsSuccess() throws Exception {
        RecordingIdempotencyStore idempotencyStore = new RecordingIdempotencyStore(
                IdempotencyStartResult.duplicate(IdempotencyState.FAILED),
                new ArrayList<>()
        );
        PublicListener listener = new PublicListener(Mono.empty());
        ReactiveRabbitBridgeMessageHandler handler = handler(listener, "handle", idempotencyStore);
        RecordingChannel channel = new RecordingChannel();

        assertThatThrownBy(() -> handler.onMessage(message(), channel.proxy()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("FAILED");
        assertThat(listener.invoked()).isFalse();
        assertThat(channel.acked()).isFalse();
        assertThat(channel.nacked()).isTrue();
    }

    @Test
    void handlerFailureCallsMarkFailedAndDoesNotAckAsSuccess() throws Exception {
        IllegalStateException handlerFailure = new IllegalStateException("handler failed");
        RecordingIdempotencyStore idempotencyStore = new RecordingIdempotencyStore(
                IdempotencyStartResult.startAccepted(),
                new ArrayList<>()
        );
        PublicListener listener = new PublicListener(Mono.error(handlerFailure));
        ReactiveRabbitBridgeMessageHandler handler = handler(listener, "handle", idempotencyStore);
        RecordingChannel channel = new RecordingChannel();

        assertThatThrownBy(() -> handler.onMessage(message(), channel.proxy()))
                .isSameAs(handlerFailure);
        assertThat(idempotencyStore.markFailed()).isTrue();
        assertThat(idempotencyStore.markFailedError()).isSameAs(handlerFailure);
        assertThat(channel.acked()).isFalse();
        assertThat(channel.nacked()).isTrue();
    }

    @Test
    void idempotencyTryStartFailureDoesNotAck() throws Exception {
        IllegalStateException tryStartFailure = new IllegalStateException("tryStart failed");
        RecordingIdempotencyStore idempotencyStore = new RecordingIdempotencyStore(
                IdempotencyStartResult.startAccepted(),
                new ArrayList<>()
        );
        idempotencyStore.failTryStartWith(tryStartFailure);
        PublicListener listener = new PublicListener(Mono.empty());
        ReactiveRabbitBridgeMessageHandler handler = handler(listener, "handle", idempotencyStore);
        RecordingChannel channel = new RecordingChannel();

        assertThatThrownBy(() -> handler.onMessage(message(), channel.proxy()))
                .isSameAs(tryStartFailure);
        assertThat(listener.invoked()).isFalse();
        assertThat(idempotencyStore.markFailed()).isFalse();
        assertThat(channel.acked()).isFalse();
        assertThat(channel.nacked()).isTrue();
    }

    @Test
    void markFailedFailureDoesNotSilentlyAckAsSuccess() throws Exception {
        IllegalStateException handlerFailure = new IllegalStateException("handler failed");
        IllegalStateException markFailedFailure = new IllegalStateException("mark failed failed");
        RecordingIdempotencyStore idempotencyStore = new RecordingIdempotencyStore(
                IdempotencyStartResult.startAccepted(),
                new ArrayList<>()
        );
        idempotencyStore.failMarkFailedWith(markFailedFailure);
        PublicListener listener = new PublicListener(Mono.error(handlerFailure));
        ReactiveRabbitBridgeMessageHandler handler = handler(listener, "handle", idempotencyStore);
        RecordingChannel channel = new RecordingChannel();

        assertThatThrownBy(() -> handler.onMessage(message(), channel.proxy()))
                .isSameAs(handlerFailure)
                .satisfies(error -> assertThat(error.getSuppressed()).contains(markFailedFailure));
        assertThat(channel.acked()).isFalse();
        assertThat(channel.nacked()).isTrue();
    }

    @Test
    void retryDlqFailureHookIsInvokedForEventFailureWhenPresent() throws Exception {
        IllegalStateException handlerFailure = new IllegalStateException("handler failed");
        RecordingIdempotencyStore idempotencyStore = new RecordingIdempotencyStore(
                IdempotencyStartResult.startAccepted(),
                new ArrayList<>()
        );
        RecordingFailureHandler failureHandler = new RecordingFailureHandler();
        PublicListener listener = new PublicListener(Mono.error(handlerFailure));
        ReactiveRabbitBridgeMessageHandler handler = handler(listener, "handle", idempotencyStore, failureHandler);
        RecordingChannel channel = new RecordingChannel();

        assertThatThrownBy(() -> handler.onMessage(message(), channel.proxy()))
                .isSameAs(handlerFailure);
        assertThat(failureHandler.failure()).isSameAs(handlerFailure);
        assertThat(failureHandler.message()).isEqualTo(messageEnvelope());
        assertThat(channel.acked()).isFalse();
        assertThat(channel.nacked()).isTrue();
    }


    @Test
    void failureHookErrorDoesNotBypassNack() throws Exception {
        AssertionError hookFailure = new AssertionError("hook failed");
        IllegalStateException handlerFailure = new IllegalStateException("handler failed");
        RecordingIdempotencyStore idempotencyStore = new RecordingIdempotencyStore(
                IdempotencyStartResult.startAccepted(),
                new ArrayList<>()
        );
        ReactiveRabbitBridgeFailureHandler failureHandler = (endpoint, reliableMessage, amqpMessage, error) -> {
            throw hookFailure;
        };
        PublicListener listener = new PublicListener(Mono.error(handlerFailure));
        ReactiveRabbitBridgeMessageHandler handler = handler(listener, "handle", idempotencyStore, failureHandler);
        RecordingChannel channel = new RecordingChannel();

        assertThatThrownBy(() -> handler.onMessage(message(), channel.proxy()))
                .isSameAs(handlerFailure)
                .satisfies(error -> assertThat(error.getSuppressed()).contains(hookFailure));
        assertThat(channel.acked()).isFalse();
        assertThat(channel.nacked()).isTrue();
    }

    @Test
    void ackFailureInvokesFailureHook() throws Exception {
        IOException ackFailure = new IOException("ack failed");
        RecordingFailureHandler failureHandler = new RecordingFailureHandler();
        PublicListener listener = new PublicListener(Mono.empty());
        ReactiveRabbitBridgeMessageHandler handler = handler(listener, "handle", null, failureHandler);
        RecordingChannel channel = new RecordingChannel();
        channel.failAckWith(ackFailure);

        assertThatThrownBy(() -> handler.onMessage(message(), channel.proxy()))
                .isSameAs(ackFailure);
        assertThat(listener.invoked()).isTrue();
        assertThat(failureHandler.failure()).isSameAs(ackFailure);
        assertThat(failureHandler.message()).isEqualTo(messageEnvelope());
        assertThat(channel.acked()).isTrue();
        assertThat(channel.nacked()).isFalse();
    }

    @Test
    void runtimeAckFailureInvokesFailureHook() throws Exception {
        IllegalStateException ackFailure = new IllegalStateException("ack failed");
        RecordingFailureHandler failureHandler = new RecordingFailureHandler();
        PublicListener listener = new PublicListener(Mono.empty());
        ReactiveRabbitBridgeMessageHandler handler = handler(listener, "handle", null, failureHandler);
        RecordingChannel channel = new RecordingChannel();
        channel.failAckWith(ackFailure);

        assertThatThrownBy(() -> handler.onMessage(message(), channel.proxy()))
                .isSameAs(ackFailure);
        assertThat(listener.invoked()).isTrue();
        assertThat(failureHandler.failure()).isSameAs(ackFailure);
        assertThat(failureHandler.message()).isEqualTo(messageEnvelope());
        assertThat(channel.acked()).isTrue();
        assertThat(channel.nacked()).isFalse();
    }

    @Test
    void deserializationFailureInvokesFailureHookAndNacks() throws Exception {
        IllegalStateException deserializeFailure = new IllegalStateException("deserialize failed");
        RecordingFailureHandler failureHandler = new RecordingFailureHandler();
        PublicListener listener = new PublicListener(Mono.empty());
        Method method = findMethod(listener.getClass(), "handle");
        ReactiveRabbitBridgeListenerEndpoint endpoint = new ReactiveRabbitBridgeListenerEndpoint(
                "listener",
                listener,
                method,
                "order.created",
                "application.order.created",
                OrderCreated.class
        );
        ReactiveRabbitBridgeMessageHandler handler = new ReactiveRabbitBridgeMessageHandler(
                endpoint,
                new FailingDeserializeSerializer(deserializeFailure),
                new ReactiveRabbitBridgeListenerMethodInvoker(),
                null,
                Duration.ofHours(24),
                failureHandler
        );
        RecordingChannel channel = new RecordingChannel();

        assertThatThrownBy(() -> handler.onMessage(message(), channel.proxy()))
                .isSameAs(deserializeFailure);
        assertThat(listener.invoked()).isFalse();
        assertThat(failureHandler.failure()).isSameAs(deserializeFailure);
        assertThat(failureHandler.message()).isNull();
        assertThat(channel.acked()).isFalse();
        assertThat(channel.nacked()).isTrue();
    }

    @Test
    void cancellationDoesNotAckAsSuccess() throws Exception {
        PublicListener listener = new PublicListener(Mono.error(new CancellationException("cancelled")));
        ReactiveRabbitBridgeMessageHandler handler = handler(listener, "handle");
        RecordingChannel channel = new RecordingChannel();

        assertThatThrownBy(() -> handler.onMessage(message(), channel.proxy()))
                .isInstanceOf(CancellationException.class);
        assertThat(channel.acked()).isFalse();
        assertThat(channel.nacked()).isTrue();
        assertThat(channel.nackMultiple()).isFalse();
        assertThat(channel.nackRequeue()).isTrue();
    }

    @Test
    void interruptedListenerCancelsInFlightHandlerAndDoesNotAck() throws Exception {
        AtomicBoolean cancelled = new AtomicBoolean();
        PublicListener listener = new PublicListener(Mono.<Void>never().doOnCancel(() -> cancelled.set(true)));
        ReactiveRabbitBridgeMessageHandler handler = handler(listener, "handle");
        RecordingChannel channel = new RecordingChannel();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread listenerThread = new Thread(() -> {
            try {
                handler.onMessage(message(), channel.proxy());
            } catch (Throwable error) {
                failure.set(error);
            }
        }, "rabbit-listener-test");

        listenerThread.start();
        await(listener::invoked);
        listenerThread.interrupt();
        listenerThread.join(1000);

        assertThat(listenerThread.isAlive()).isFalse();
        assertThat(failure.get()).isInstanceOf(CancellationException.class);
        assertThat(cancelled).isTrue();
        assertThat(channel.acked()).isFalse();
        assertThat(channel.nacked()).isTrue();
        assertThat(channel.nackMultiple()).isFalse();
        assertThat(channel.nackRequeue()).isTrue();
    }

    @Test
    void listenerWorkDoesNotUseReactorParallelOrCommonPoolByDefault() throws Exception {
        AtomicReference<String> threadName = new AtomicReference<>();
        PublicListener listener = new PublicListener(Mono.fromRunnable(() -> threadName.set(Thread.currentThread().getName())));
        ReactiveRabbitBridgeMessageHandler handler = handler(listener, "handle");

        handler.onMessage(message(), new RecordingChannel().proxy());

        assertThat(threadName.get())
                .isEqualTo(Thread.currentThread().getName())
                .doesNotContain("parallel")
                .doesNotContain("ForkJoinPool");
    }

    @Test
    void listenerBridgeDoesNotUseStrategyBAsyncAckOrRetryDlq() throws IOException {
        String sources = mainSources();

        assertThat(sources)
                .doesNotContain("AsyncRabbitTemplate")
                .doesNotContain("ReactiveReliableRpc")
                .doesNotContain("Outbox")
                .doesNotContain("doOnSuccess")
                .doesNotContain("doOnError")
                .doesNotContain("subscribe(")
                .doesNotContain("RetryStrategy")
                .doesNotContain("Dlq")
                .doesNotContain("DeadLetter");
    }

    private static ReactiveRabbitBridgeMessageHandler handler(Object bean, String methodName) throws Exception {
        return handler(bean, methodName, null);
    }

    private static ReactiveRabbitBridgeMessageHandler handler(
            Object bean,
            String methodName,
            ReactiveIdempotencyStore idempotencyStore
    ) throws Exception {
        return handler(bean, methodName, idempotencyStore, ReactiveRabbitBridgeFailureHandler.noop());
    }

    private static ReactiveRabbitBridgeMessageHandler handler(
            Object bean,
            String methodName,
            ReactiveIdempotencyStore idempotencyStore,
            ReactiveRabbitBridgeFailureHandler failureHandler
    ) throws Exception {
        Method method = findMethod(bean.getClass(), methodName);
        ReactiveRabbitBridgeListenerEndpoint endpoint = new ReactiveRabbitBridgeListenerEndpoint(
                "listener",
                bean,
                method,
                "order.created",
                "application.order.created",
                OrderCreated.class
        );
        return new ReactiveRabbitBridgeMessageHandler(
                endpoint,
                new RecordingSerializer(messageEnvelope()),
                new ReactiveRabbitBridgeListenerMethodInvoker(),
                idempotencyStore,
                Duration.ofHours(24),
                failureHandler
        );
    }

    private static Method findMethod(Class<?> type, String methodName) {
        for (Method method : type.getDeclaredMethods()) {
            if (method.getName().equals(methodName)) {
                return method;
            }
        }
        throw new IllegalArgumentException("No method named " + methodName);
    }

    private static Message message() {
        MessageProperties properties = new MessageProperties();
        properties.setDeliveryTag(42L);
        return new Message(new byte[]{1, 2, 3}, properties);
    }

    private static ReliableMessage<OrderCreated> messageEnvelope() {
        return new ReliableMessage<>(
                "message-1",
                "order.created",
                "order-1",
                "idempotency-1",
                "correlation-1",
                "trace-1",
                Instant.parse("2026-05-22T00:00:00Z"),
                Map.of(),
                new OrderCreated("order-1")
        );
    }

    private static void invoke(ReactiveRabbitBridgeMessageHandler handler, RecordingChannel channel) {
        try {
            handler.onMessage(message(), channel.proxy());
        } catch (IOException error) {
            throw new UncheckedIOException(error);
        }
    }

    private static void await(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        assertThat(condition.getAsBoolean()).isTrue();
    }

    private static String mainSources() throws IOException {
        StringBuilder builder = new StringBuilder();
        try (var paths = Files.walk(Path.of("src/main/java"))) {
            paths.filter(path -> path.toString().endsWith(".java"))
                    .sorted()
                    .forEach(path -> {
                        try {
                            builder.append(Files.readString(path)).append('\n');
                        } catch (IOException error) {
                            throw new IllegalStateException(error);
                        }
                    });
        }
        return builder.toString();
    }

    private static final class PublicListener {
        private final Mono<Void> result;
        private final AtomicBoolean invoked = new AtomicBoolean();

        private PublicListener(Mono<Void> result) {
            this.result = result;
        }

        Mono<Void> handle(ReliableMessage<OrderCreated> message) {
            invoked.set(true);
            return result;
        }

        boolean invoked() {
            return invoked.get();
        }
    }

    private static final class RecordingOrderListener {
        private final List<String> events;
        private final Mono<Void> result;

        private RecordingOrderListener(List<String> events, Mono<Void> result) {
            this.events = events;
            this.result = result;
        }

        Mono<Void> handle(ReliableMessage<OrderCreated> message) {
            events.add("handler");
            return result;
        }
    }

    private static final class PrivateListener {
        private final AtomicBoolean invoked = new AtomicBoolean();

        private Mono<Void> handle(ReliableMessage<OrderCreated> message) {
            invoked.set(true);
            return Mono.empty();
        }

        boolean invoked() {
            return invoked.get();
        }
    }

    private static final class RecordingSerializer implements MessageSerializer {
        private final ReliableMessage<OrderCreated> message;

        private RecordingSerializer(ReliableMessage<OrderCreated> message) {
            this.message = message;
        }

        @Override
        public <T> byte[] serialize(ReliableMessage<T> message) {
            throw new UnsupportedOperationException("serialize is not used by listener tests");
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> ReliableMessage<T> deserialize(byte[] content, Class<T> payloadType) {
            return (ReliableMessage<T>) message;
        }
    }

    private static final class FailingDeserializeSerializer implements MessageSerializer {
        private final RuntimeException failure;

        private FailingDeserializeSerializer(RuntimeException failure) {
            this.failure = failure;
        }

        @Override
        public <T> byte[] serialize(ReliableMessage<T> message) {
            throw new UnsupportedOperationException("serialize is not used by listener tests");
        }

        @Override
        public <T> ReliableMessage<T> deserialize(byte[] content, Class<T> payloadType) {
            throw failure;
        }
    }

    private static final class RecordingChannel {
        private final AtomicBoolean acked = new AtomicBoolean();
        private final AtomicBoolean nacked = new AtomicBoolean();
        private final AtomicReference<Long> ackTimeNanos = new AtomicReference<>();
        private final AtomicReference<Boolean> nackMultiple = new AtomicReference<>();
        private final AtomicReference<Boolean> nackRequeue = new AtomicReference<>();
        private final List<String> events;
        private RuntimeException nackFailure;
        private IOException ackIoFailure;
        private RuntimeException ackRuntimeFailure;

        private RecordingChannel() {
            this(null);
        }

        private RecordingChannel(List<String> events) {
            this.events = events;
        }

        Channel proxy() {
            return (Channel) Proxy.newProxyInstance(
                    Channel.class.getClassLoader(),
                    new Class<?>[]{Channel.class},
                    (proxy, method, args) -> {
                        if (method.getName().equals("basicAck")) {
                            acked.set(true);
                            ackTimeNanos.set(System.nanoTime());
                            if (events != null) {
                                events.add("ack");
                            }
                            if (ackIoFailure != null) {
                                throw ackIoFailure;
                            }
                            if (ackRuntimeFailure != null) {
                                throw ackRuntimeFailure;
                            }
                            return null;
                        }
                        if (method.getName().equals("basicNack")) {
                            nacked.set(true);
                            nackMultiple.set((Boolean) args[1]);
                            nackRequeue.set((Boolean) args[2]);
                            if (events != null) {
                                events.add("nack");
                            }
                            if (nackFailure != null) {
                                throw nackFailure;
                            }
                            return null;
                        }
                        if (method.getReturnType() == Boolean.TYPE) {
                            return false;
                        }
                        if (method.getReturnType() == Integer.TYPE) {
                            return 0;
                        }
                        if (method.getReturnType() == Long.TYPE) {
                            return 0L;
                        }
                        return null;
                    }
            );
        }

        boolean acked() {
            return acked.get();
        }

        long ackTimeNanos() {
            return ackTimeNanos.get();
        }

        void failNackWith(RuntimeException failure) {
            this.nackFailure = failure;
        }

        void failAckWith(IOException failure) {
            this.ackIoFailure = failure;
        }

        void failAckWith(RuntimeException failure) {
            this.ackRuntimeFailure = failure;
        }

        boolean nacked() {
            return nacked.get();
        }

        boolean nackMultiple() {
            return Boolean.TRUE.equals(nackMultiple.get());
        }

        boolean nackRequeue() {
            return Boolean.TRUE.equals(nackRequeue.get());
        }
    }


    private static final class RecordingIdempotencyStore implements ReactiveIdempotencyStore {
        private final IdempotencyStartResult startResult;
        private final List<String> events;
        private final AtomicBoolean markSuccess = new AtomicBoolean();
        private final AtomicBoolean markFailed = new AtomicBoolean();
        private final AtomicReference<String> tryStartKey = new AtomicReference<>();
        private final AtomicReference<Throwable> markFailedError = new AtomicReference<>();
        private RuntimeException tryStartFailure;
        private RuntimeException markSuccessFailure;
        private RuntimeException markFailedFailure;

        private RecordingIdempotencyStore(IdempotencyStartResult startResult, List<String> events) {
            this.startResult = startResult;
            this.events = events;
        }

        @Override
        public Mono<IdempotencyStartResult> tryStart(String key, Duration ttl) {
            return Mono.defer(() -> {
                events.add("tryStart");
                tryStartKey.set(key);
                if (tryStartFailure != null) {
                    return Mono.error(tryStartFailure);
                }
                return Mono.just(startResult);
            });
        }

        @Override
        public Mono<Void> markSuccess(String key) {
            return Mono.defer(() -> {
                events.add("markSuccess");
                markSuccess.set(true);
                if (markSuccessFailure != null) {
                    return Mono.error(markSuccessFailure);
                }
                return Mono.empty();
            });
        }

        @Override
        public Mono<Void> markFailed(String key, Throwable error) {
            return Mono.defer(() -> {
                events.add("markFailed");
                markFailed.set(true);
                markFailedError.set(error);
                if (markFailedFailure != null) {
                    return Mono.error(markFailedFailure);
                }
                return Mono.empty();
            });
        }

        void failTryStartWith(RuntimeException failure) {
            this.tryStartFailure = failure;
        }

        void failMarkSuccessWith(RuntimeException failure) {
            this.markSuccessFailure = failure;
        }

        void failMarkFailedWith(RuntimeException failure) {
            this.markFailedFailure = failure;
        }

        String tryStartKey() {
            return tryStartKey.get();
        }

        boolean markSuccess() {
            return markSuccess.get();
        }

        boolean markFailed() {
            return markFailed.get();
        }

        Throwable markFailedError() {
            return markFailedError.get();
        }
    }

    private static final class RecordingFailureHandler implements ReactiveRabbitBridgeFailureHandler {
        private final AtomicReference<ReliableMessage<?>> message = new AtomicReference<>();
        private final AtomicReference<Throwable> failure = new AtomicReference<>();

        @Override
        public void handleFailure(
                ReactiveRabbitBridgeListenerEndpoint endpoint,
                ReliableMessage<?> reliableMessage,
                Message amqpMessage,
                Throwable error
        ) {
            message.set(reliableMessage);
            failure.set(error);
        }

        ReliableMessage<?> message() {
            return message.get();
        }

        Throwable failure() {
            return failure.get();
        }
    }

    @FunctionalInterface
    private interface BooleanSupplier {
        boolean getAsBoolean();
    }

    record OrderCreated(String orderId) {
    }
}
