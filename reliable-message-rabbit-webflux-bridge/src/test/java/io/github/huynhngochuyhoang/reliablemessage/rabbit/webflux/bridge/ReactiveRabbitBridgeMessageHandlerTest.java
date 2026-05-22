package io.github.huynhngochuyhoang.reliablemessage.rabbit.webflux.bridge;

import com.rabbitmq.client.Channel;
import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessage;
import io.github.huynhngochuyhoang.reliablemessage.core.serialization.MessageSerializer;
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
    }

    @Test
    void cancellationDoesNotAckAsSuccess() throws Exception {
        PublicListener listener = new PublicListener(Mono.error(new CancellationException("cancelled")));
        ReactiveRabbitBridgeMessageHandler handler = handler(listener, "handle");
        RecordingChannel channel = new RecordingChannel();

        assertThatThrownBy(() -> handler.onMessage(message(), channel.proxy()))
                .isInstanceOf(CancellationException.class);
        assertThat(channel.acked()).isFalse();
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
                .doesNotContain("doOnSuccess")
                .doesNotContain("doOnError")
                .doesNotContain("subscribe(")
                .doesNotContain("RetryStrategy")
                .doesNotContain("Dlq")
                .doesNotContain("DeadLetter");
    }

    private static ReactiveRabbitBridgeMessageHandler handler(Object bean, String methodName) throws Exception {
        Method method = findMethod(bean.getClass(), methodName);
        ReactiveRabbitBridgeListenerEndpoint endpoint = new ReactiveRabbitBridgeListenerEndpoint(
                "listener",
                bean,
                method,
                "order.created",
                "application.order.created",
                OrderCreated.class
        );
        return new ReactiveRabbitBridgeMessageHandler(endpoint, new RecordingSerializer(messageEnvelope()), new ReactiveRabbitBridgeListenerMethodInvoker());
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

    private static final class RecordingChannel {
        private final AtomicBoolean acked = new AtomicBoolean();
        private final AtomicReference<Long> ackTimeNanos = new AtomicReference<>();

        Channel proxy() {
            return (Channel) Proxy.newProxyInstance(
                    Channel.class.getClassLoader(),
                    new Class<?>[]{Channel.class},
                    (proxy, method, args) -> {
                        if (method.getName().equals("basicAck")) {
                            acked.set(true);
                            ackTimeNanos.set(System.nanoTime());
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
    }

    @FunctionalInterface
    private interface BooleanSupplier {
        boolean getAsBoolean();
    }

    record OrderCreated(String orderId) {
    }
}
