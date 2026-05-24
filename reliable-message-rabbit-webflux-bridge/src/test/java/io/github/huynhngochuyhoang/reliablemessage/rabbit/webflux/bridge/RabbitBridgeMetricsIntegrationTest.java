package io.github.huynhngochuyhoang.reliablemessage.rabbit.webflux.bridge;

import com.rabbitmq.client.Channel;
import io.github.huynhngochuyhoang.reliablemessage.core.PublishOptions;
import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessage;
import io.github.huynhngochuyhoang.reliablemessage.core.serialization.MessageSerializer;
import io.github.huynhngochuyhoang.reliablemessage.webflux.IdempotencyStartResult;
import io.github.huynhngochuyhoang.reliablemessage.webflux.IdempotencyState;
import io.github.huynhngochuyhoang.reliablemessage.webflux.ReactiveIdempotencyStore;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RabbitBridgeMetricsIntegrationTest {

    @Test
    void publishSuccessCounterIncrements() {
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        RecordingRabbitTemplate rabbitTemplate = new RecordingRabbitTemplate();
        ReactiveRabbitBridgePublisher publisher = publisher(rabbitTemplate, new RecordingSerializer(), directProvider(), 1, meterRegistry);

        publisher.publish("order.created", new OrderCreated("order-1"), PublishOptions.empty()).block(Duration.ofSeconds(1));

        assertThat(counter(meterRegistry, "message_rabbit_bridge_publish_total", "order.created", "success")).isEqualTo(1.0);
    }

    @Test
    void publishFailureCounterIncrementsAndFailureStillPropagates() {
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        RecordingRabbitTemplate rabbitTemplate = new RecordingRabbitTemplate();
        rabbitTemplate.failWith(new AmqpException("publish failed"));
        ReactiveRabbitBridgePublisher publisher = publisher(rabbitTemplate, new RecordingSerializer(), directProvider(), 1, meterRegistry);

        assertThatThrownBy(() -> publisher.publish("order.created", new OrderCreated("order-1"), PublishOptions.empty())
                .block(Duration.ofSeconds(1)))
                .isInstanceOf(AmqpException.class);

        assertThat(counter(meterRegistry, "message_rabbit_bridge_publish_total", "order.created", "failure")).isEqualTo(1.0);
        assertThat(meterRegistry.find("message_rabbit_bridge_publish_total")
                .tag("event_name", "order.created")
                .tag("status", "success")
                .counter()).isNull();
    }

    @Test
    void executorRejectionCounterIncrements() {
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        RecordingRabbitTemplate rabbitTemplate = new RecordingRabbitTemplate();
        RejectingExecutorService executor = new RejectingExecutorService();
        ReactiveRabbitBridgePublisher publisher = publisher(rabbitTemplate, new RecordingSerializer(), provider(executor), 1, meterRegistry);

        assertThatThrownBy(() -> publisher.publish("order.created", new OrderCreated("order-1"), PublishOptions.empty())
                .block(Duration.ofSeconds(1)))
                .isInstanceOf(RabbitBridgeRejectedException.class);

        assertThat(counter(meterRegistry, "message_rabbit_bridge_executor_rejected_total", "order.created", "rejected")).isEqualTo(1.0);
    }

    @Test
    void consumeSuccessCounterIncrements() throws Exception {
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        PublicListener listener = new PublicListener(Mono.empty());
        ReactiveRabbitBridgeMessageHandler handler = handler(listener, null, meterRegistry);

        handler.onMessage(amqpMessage(), channel());

        assertThat(counter(meterRegistry, "message_rabbit_bridge_consume_total", "order.created", "success")).isEqualTo(1.0);
    }

    @Test
    void consumeFailureCounterIncrementsAndFailureStillPropagates() throws Exception {
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        IllegalStateException failure = new IllegalStateException("handler failed");
        PublicListener listener = new PublicListener(Mono.error(failure));
        ReactiveRabbitBridgeMessageHandler handler = handler(listener, null, meterRegistry);

        assertThatThrownBy(() -> handler.onMessage(amqpMessage(), channel()))
                .isSameAs(failure);

        assertThat(counter(meterRegistry, "message_rabbit_bridge_consume_total", "order.created", "failure")).isEqualTo(1.0);
        assertThat(meterRegistry.find("message_rabbit_bridge_consume_total")
                .tag("event_name", "order.created")
                .tag("status", "success")
                .counter()).isNull();
    }

    @Test
    void duplicateCounterIncrements() throws Exception {
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        PublicListener listener = new PublicListener(Mono.empty());
        ReactiveIdempotencyStore idempotencyStore = new DuplicateSuccessIdempotencyStore();
        ReactiveRabbitBridgeMessageHandler handler = handler(listener, idempotencyStore, meterRegistry);

        handler.onMessage(amqpMessage(), channel());

        assertThat(listener.invoked()).isFalse();
        assertThat(counter(meterRegistry, "message_rabbit_bridge_duplicate_total", "order.created", "success")).isEqualTo(1.0);
    }

    @Test
    void noRpcMetricsOrAsyncRabbitTemplateUsageExists() throws IOException {
        String source = mainSources();

        assertThat(source)
                .doesNotContain("AsyncRabbitTemplate")
                .doesNotContain("rpc_")
                .doesNotContain("ReactiveReliableRpc");
    }

    private static ReactiveRabbitBridgePublisher publisher(
            RabbitTemplate rabbitTemplate,
            MessageSerializer serializer,
            RabbitBridgeExecutorProvider executorProvider,
            int maxConcurrency,
            SimpleMeterRegistry meterRegistry
    ) {
        RabbitWebFluxBridgeProperties properties = new RabbitWebFluxBridgeProperties();
        properties.getRabbit().setExchange("app.events");
        return new ReactiveRabbitBridgePublisher(
                rabbitTemplate,
                serializer,
                properties,
                executorProvider,
                new RabbitBridgeConcurrencyGuard(maxConcurrency),
                Clock.fixed(Instant.parse("2026-05-22T00:00:00Z"), ZoneOffset.UTC),
                new RabbitBridgeEventLoopDetector(),
                RabbitBridgeSafetyReporter.noop(),
                new RabbitBridgeMetrics(meterRegistry, RabbitWebFluxBridgeProperties.ExecutorMode.PLATFORM)
        );
    }

    private static ReactiveRabbitBridgeMessageHandler handler(
            PublicListener listener,
            ReactiveIdempotencyStore idempotencyStore,
            SimpleMeterRegistry meterRegistry
    ) throws NoSuchMethodException {
        Method method = PublicListener.class.getDeclaredMethod("handle", ReliableMessage.class);
        ReactiveRabbitBridgeListenerEndpoint endpoint = new ReactiveRabbitBridgeListenerEndpoint(
                "listener",
                listener,
                method,
                "order.created",
                "application.order.created",
                OrderCreated.class
        );
        return new ReactiveRabbitBridgeMessageHandler(
                endpoint,
                new RecordingSerializer(),
                new ReactiveRabbitBridgeListenerMethodInvoker(),
                idempotencyStore,
                Duration.ofHours(24),
                ReactiveRabbitBridgeFailureHandler.noop(),
                new RabbitBridgeMetrics(meterRegistry, RabbitWebFluxBridgeProperties.ExecutorMode.PLATFORM)
        );
    }

    private static Message amqpMessage() {
        MessageProperties properties = new MessageProperties();
        properties.setDeliveryTag(42L);
        return new Message(new byte[]{1, 2, 3}, properties);
    }

    private static Channel channel() {
        return (Channel) Proxy.newProxyInstance(
                Channel.class.getClassLoader(),
                new Class<?>[]{Channel.class},
                (proxy, method, args) -> null
        );
    }

    private static String mainSources() throws IOException {
        StringBuilder builder = new StringBuilder();
        try (var paths = java.nio.file.Files.walk(java.nio.file.Path.of("src/main/java"))) {
            paths.filter(path -> path.toString().endsWith(".java"))
                    .sorted()
                    .forEach(path -> {
                        try {
                            builder.append(java.nio.file.Files.readString(path)).append("\n");
                        } catch (IOException error) {
                            throw new IllegalStateException(error);
                        }
                    });
        }
        return builder.toString();
    }

    private static RabbitBridgeExecutorProvider directProvider() {
        return provider(new DirectExecutorService());
    }

    private static RabbitBridgeExecutorProvider provider(ExecutorService executor) {
        return new RabbitBridgeExecutorProvider() {
            @Override
            public ExecutorService getExecutor() {
                return executor;
            }

            @Override
            public void close() {
                executor.shutdownNow();
            }
        };
    }

    private static double counter(SimpleMeterRegistry meterRegistry, String name, String eventName, String status) {
        return meterRegistry.find(name)
                .tag("runtime", "webflux-bridge")
                .tag("transport", "rabbit")
                .tag("executor_mode", "platform")
                .tag("event_name", eventName)
                .tag("status", status)
                .counter()
                .count();
    }

    private static final class RecordingRabbitTemplate extends RabbitTemplate {
        private RuntimeException failure;

        @Override
        public void convertAndSend(String exchange, String routingKey, Object message) throws AmqpException {
            if (failure != null) {
                throw failure;
            }
        }

        void failWith(RuntimeException failure) {
            this.failure = failure;
        }
    }

    private static final class RecordingSerializer implements MessageSerializer {
        @Override
        public <T> byte[] serialize(ReliableMessage<T> message) {
            return new byte[]{1, 2, 3};
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> ReliableMessage<T> deserialize(byte[] content, Class<T> payloadType) {
            return (ReliableMessage<T>) new ReliableMessage<>(
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
    }

    private static final class DuplicateSuccessIdempotencyStore implements ReactiveIdempotencyStore {
        @Override
        public Mono<IdempotencyStartResult> tryStart(String key, Duration ttl) {
            return Mono.just(IdempotencyStartResult.duplicate(IdempotencyState.SUCCESS));
        }

        @Override
        public Mono<Void> markSuccess(String key) {
            return Mono.empty();
        }

        @Override
        public Mono<Void> markFailed(String key, Throwable error) {
            return Mono.empty();
        }
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

    private static class DirectExecutorService extends AbstractExecutorService {
        private boolean shutdown;

        @Override
        public void shutdown() {
            shutdown = true;
        }

        @Override
        public java.util.List<Runnable> shutdownNow() {
            shutdown = true;
            return java.util.List.of();
        }

        @Override
        public boolean isShutdown() {
            return shutdown;
        }

        @Override
        public boolean isTerminated() {
            return shutdown;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) {
            return shutdown;
        }

        @Override
        public void execute(Runnable command) {
            command.run();
        }
    }

    private static final class RejectingExecutorService extends DirectExecutorService {
        @Override
        public void execute(Runnable command) {
            throw new java.util.concurrent.RejectedExecutionException("rejected");
        }
    }

    record OrderCreated(String orderId) {
    }
}
