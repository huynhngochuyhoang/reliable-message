package io.github.huynhngochuyhoang.reliablemessage.rabbit.webflux.bridge;

import io.github.huynhngochuyhoang.reliablemessage.core.PublishOptions;
import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessage;
import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessageHeaders;
import io.github.huynhngochuyhoang.reliablemessage.core.serialization.MessageSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import reactor.core.Disposable;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ReactiveRabbitBridgePublisherTest {

    @Test
    void publishCallsRabbitTemplateConvertAndSend() {
        RecordingRabbitTemplate rabbitTemplate = new RecordingRabbitTemplate();
        RecordingSerializer serializer = new RecordingSerializer();
        ReactiveRabbitBridgePublisher publisher = publisher(rabbitTemplate, serializer, platformProvider(1, 1), 1);

        publisher.publish("order.created", new OrderCreated("order-1"), PublishOptions.builder()
                        .aggregateId("order-1")
                        .idempotencyKey("event-1")
                        .correlationId("correlation-1")
                        .partitionKey("order-1")
                        .build())
                .block(Duration.ofSeconds(1));

        assertThat(rabbitTemplate.exchange()).isEqualTo("app.events");
        assertThat(rabbitTemplate.routingKey()).isEqualTo("order.created");
        assertThat(rabbitTemplate.message()).isInstanceOf(Message.class);
        assertThat(serializer.message().eventName()).isEqualTo("order.created");
        assertThat(serializer.message().aggregateId()).isEqualTo("order-1");
        assertThat(serializer.message().headers()).containsEntry(ReliableMessageHeaders.CORRELATION_ID, "correlation-1");
    }

    @Test
    void publishRunsOnBridgeExecutorAndNotCallerThread() {
        RecordingRabbitTemplate rabbitTemplate = new RecordingRabbitTemplate();
        ReactiveRabbitBridgePublisher publisher = publisher(rabbitTemplate, new RecordingSerializer(), platformProvider(1, 1), 1);
        String callerThread = Thread.currentThread().getName();

        publisher.publish("order.created", new OrderCreated("order-1"), PublishOptions.empty())
                .block(Duration.ofSeconds(1));

        assertThat(rabbitTemplate.threadName())
                .startsWith("reliable-message-rabbit-bridge-platform-")
                .isNotEqualTo(callerThread)
                .doesNotContain("parallel")
                .doesNotContain("ForkJoinPool");
    }

    @Test
    void permitReleasedAfterSuccessfulPublish() {
        RecordingRabbitTemplate rabbitTemplate = new RecordingRabbitTemplate();
        ReactiveRabbitBridgePublisher publisher = publisher(rabbitTemplate, new RecordingSerializer(), platformProvider(1, 1), 1);

        publisher.publish("order.created", new OrderCreated("order-1"), PublishOptions.empty()).block(Duration.ofSeconds(1));
        publisher.publish("order.created", new OrderCreated("order-2"), PublishOptions.empty()).block(Duration.ofSeconds(1));

        assertThat(rabbitTemplate.calls()).isEqualTo(2);
    }

    @Test
    void permitReleasedAfterPublishFailure() {
        RecordingRabbitTemplate rabbitTemplate = new RecordingRabbitTemplate();
        rabbitTemplate.failOnce(new AmqpException("publish failed"));
        ReactiveRabbitBridgePublisher publisher = publisher(rabbitTemplate, new RecordingSerializer(), platformProvider(1, 1), 1);

        assertThatThrownBy(() -> publisher.publish("order.created", new OrderCreated("order-1"), PublishOptions.empty())
                .block(Duration.ofSeconds(1)))
                .isInstanceOf(AmqpException.class);

        publisher.publish("order.created", new OrderCreated("order-2"), PublishOptions.empty()).block(Duration.ofSeconds(1));
        assertThat(rabbitTemplate.calls()).isEqualTo(2);
    }

    @Test
    void permitReleasedAfterCancellation() {
        RecordingRabbitTemplate rabbitTemplate = new RecordingRabbitTemplate();
        QueuingExecutorService executor = new QueuingExecutorService();
        ReactiveRabbitBridgePublisher publisher = publisher(rabbitTemplate, new RecordingSerializer(), queueProvider(executor), 1);

        Disposable subscription = publisher.publish("order.created", new OrderCreated("order-1"), PublishOptions.empty())
                .subscribe();
        subscription.dispose();

        publisher.publish("order.created", new OrderCreated("order-2"), PublishOptions.empty()).subscribe();
        assertThat(executor.executeCalls()).isEqualTo(2);
    }

    @Test
    void saturationReturnsMonoErrorAndDoesNotQueueMoreWork() {
        RecordingRabbitTemplate rabbitTemplate = new RecordingRabbitTemplate();
        QueuingExecutorService executor = new QueuingExecutorService();
        ReactiveRabbitBridgePublisher publisher = publisher(rabbitTemplate, new RecordingSerializer(), queueProvider(executor), 1);

        publisher.publish("order.created", new OrderCreated("order-1"), PublishOptions.empty()).subscribe();

        assertThatThrownBy(() -> publisher.publish("order.created", new OrderCreated("order-2"), PublishOptions.empty())
                .block(Duration.ofSeconds(1)))
                .isInstanceOf(RabbitBridgeRejectedException.class);
        assertThat(executor.executeCalls()).isEqualTo(1);
    }

    @Test
    void serializationFailureDoesNotCallRabbitTemplateOrLeakPermit() {
        RecordingRabbitTemplate rabbitTemplate = new RecordingRabbitTemplate();
        RecordingSerializer serializer = new RecordingSerializer();
        serializer.failOnce(new IllegalStateException("serialize failed"));
        ReactiveRabbitBridgePublisher publisher = publisher(rabbitTemplate, serializer, platformProvider(1, 1), 1);

        assertThatThrownBy(() -> publisher.publish("order.created", new OrderCreated("order-1"), PublishOptions.empty())
                .block(Duration.ofSeconds(1)))
                .isInstanceOf(IllegalStateException.class);
        assertThat(rabbitTemplate.calls()).isZero();

        publisher.publish("order.created", new OrderCreated("order-2"), PublishOptions.empty()).block(Duration.ofSeconds(1));
        assertThat(rabbitTemplate.calls()).isEqualTo(1);
    }

    @Test
    void mainSourceDoesNotUseAsyncRabbitTemplate() throws IOException {
        String source = mainSources();

        assertThat(source).doesNotContain("AsyncRabbitTemplate");
    }

    @Test
    void publisherDoesNotUseReactorParallelOrCommonPool() throws IOException {
        String source = Files.readString(Path.of("src/main/java/io/github/huynhngochuyhoang/reliablemessage/rabbit/webflux/bridge/ReactiveRabbitBridgePublisher.java"));

        assertThat(source)
                .doesNotContain("Schedulers.parallel")
                .doesNotContain("Schedulers.boundedElastic")
                .doesNotContain("ForkJoinPool")
                .doesNotContain("commonPool")
                .doesNotContain("Mono.just(rabbitTemplate");
    }

    private static ReactiveRabbitBridgePublisher publisher(
            RabbitTemplate rabbitTemplate,
            MessageSerializer serializer,
            RabbitBridgeExecutorProvider executorProvider,
            int maxConcurrency
    ) {
        RabbitWebFluxBridgeProperties properties = new RabbitWebFluxBridgeProperties();
        properties.getRabbit().setExchange("app.events");
        return new ReactiveRabbitBridgePublisher(
                rabbitTemplate,
                serializer,
                properties,
                executorProvider,
                new RabbitBridgeConcurrencyGuard(maxConcurrency),
                Clock.fixed(Instant.parse("2026-05-22T00:00:00Z"), ZoneOffset.UTC)
        );
    }

    private static RabbitBridgeExecutorProvider platformProvider(int workerThreads, int queueCapacity) {
        return new TestRabbitBridgeExecutorProvider(workerThreads, queueCapacity);
    }

    private static RabbitBridgeExecutorProvider queueProvider(ExecutorService executor) {
        return new RabbitBridgeExecutorProvider() {
            
            public ExecutorService getExecutor() {
                return executor;
            }
            
            public void close() {
                executor.shutdownNow();
            }
        };
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

    private static final class RecordingRabbitTemplate extends RabbitTemplate {
        private final AtomicInteger calls = new AtomicInteger();
        private final AtomicReference<String> exchange = new AtomicReference<>();
        private final AtomicReference<String> routingKey = new AtomicReference<>();
        private final AtomicReference<Object> message = new AtomicReference<>();
        private final AtomicReference<String> threadName = new AtomicReference<>();
        private RuntimeException nextFailure;

        @Override
        public void convertAndSend(String exchange, String routingKey, Object message) throws AmqpException {
            calls.incrementAndGet();
            this.exchange.set(exchange);
            this.routingKey.set(routingKey);
            this.message.set(message);
            this.threadName.set(Thread.currentThread().getName());
            RuntimeException failure = nextFailure;
            nextFailure = null;
            if (failure != null) {
                throw failure;
            }
        }

        void failOnce(RuntimeException failure) {
            this.nextFailure = failure;
        }

        int calls() {
            return calls.get();
        }

        String exchange() {
            return exchange.get();
        }

        String routingKey() {
            return routingKey.get();
        }

        Object message() {
            return message.get();
        }

        String threadName() {
            return threadName.get();
        }
    }

    private static final class RecordingSerializer implements MessageSerializer {
        private final AtomicReference<ReliableMessage<?>> message = new AtomicReference<>();
        private RuntimeException nextFailure;

        @Override
        public <T> byte[] serialize(ReliableMessage<T> message) {
            RuntimeException failure = nextFailure;
            nextFailure = null;
            if (failure != null) {
                throw failure;
            }
            this.message.set(message);
            return new byte[]{1, 2, 3};
        }

        @Override
        public <T> ReliableMessage<T> deserialize(byte[] content, Class<T> payloadType) {
            throw new UnsupportedOperationException("deserialize is not used by publisher tests");
        }

        void failOnce(RuntimeException failure) {
            this.nextFailure = failure;
        }

        ReliableMessage<?> message() {
            return message.get();
        }
    }

    private static final class TestRabbitBridgeExecutorProvider implements RabbitBridgeExecutorProvider {
        private final ExecutorService executor;

        private TestRabbitBridgeExecutorProvider(int workerThreads, int queueCapacity) {
            AtomicInteger threadNumber = new AtomicInteger();
            ThreadFactory threadFactory = task -> {
                Thread thread = new Thread(task, "reliable-message-rabbit-bridge-platform-test-" + threadNumber.incrementAndGet());
                thread.setDaemon(true);
                return thread;
            };
            this.executor = new ThreadPoolExecutor(
                    workerThreads,
                    workerThreads,
                    0L,
                    TimeUnit.MILLISECONDS,
                    new ArrayBlockingQueue<>(queueCapacity),
                    threadFactory
            );
        }

        @Override
        public ExecutorService getExecutor() {
            return executor;
        }

        @Override
        public void close() {
            executor.shutdownNow();
        }
    }

    private static final class QueuingExecutorService extends AbstractExecutorService {
        private final List<Runnable> tasks = new ArrayList<>();
        private final AtomicInteger executeCalls = new AtomicInteger();
        private boolean shutdown;

        @Override
        public void shutdown() {
            shutdown = true;
        }

        @Override
        public List<Runnable> shutdownNow() {
            shutdown = true;
            return List.copyOf(tasks);
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
            executeCalls.incrementAndGet();
            tasks.add(command);
        }

        int executeCalls() {
            return executeCalls.get();
        }
    }

    record OrderCreated(String orderId) {
    }
}
