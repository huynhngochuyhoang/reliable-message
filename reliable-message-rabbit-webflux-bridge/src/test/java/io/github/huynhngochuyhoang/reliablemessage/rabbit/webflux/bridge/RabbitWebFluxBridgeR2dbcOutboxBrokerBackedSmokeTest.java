package io.github.huynhngochuyhoang.reliablemessage.rabbit.webflux.bridge;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.huynhngochuyhoang.reliablemessage.core.MessageStatus;
import io.github.huynhngochuyhoang.reliablemessage.core.PublishOptions;
import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessage;
import io.github.huynhngochuyhoang.reliablemessage.core.serialization.MessageSerializer;
import io.github.huynhngochuyhoang.reliablemessage.outbox.r2dbc.R2dbcOutboxProperties;
import io.github.huynhngochuyhoang.reliablemessage.outbox.r2dbc.R2dbcOutboxStore;
import io.github.huynhngochuyhoang.reliablemessage.outbox.r2dbc.ReactiveOutboxFlushScheduler;
import io.github.huynhngochuyhoang.reliablemessage.webflux.*;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.r2dbc.spi.ConnectionFactories;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.core.env.Environment;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.publisher.Mono;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(
        classes = RabbitWebFluxBridgeR2dbcOutboxBrokerBackedSmokeTest.SampleApplication.class,
        properties = {
                "message.reliability.transport=rabbit",
                "message.reliability.service-name=webflux-outbox-orders",
                "message.reliability.rabbit.exchange=s5.webflux.rabbit.outbox.events",
                "message.reliability.rabbit.bridge.executor-mode=platform",
                "message.reliability.rabbit.bridge.worker-threads=2",
                "message.reliability.rabbit.bridge.queue-capacity=2",
                "message.reliability.rabbit.bridge.max-concurrency=4",
                "message.reliability.outbox.enabled=true",
                "message.reliability.outbox.flush-enabled=false",
                "message.reliability.outbox.batch-size=4",
                "message.reliability.outbox.publish-timeout=10s",
                "message.reliability.outbox.schema.payload-bytes-column-type=bytea"
        }
)
class RabbitWebFluxBridgeR2dbcOutboxBrokerBackedSmokeTest {

    @Container
    static final RabbitMQContainer rabbit = new RabbitMQContainer("rabbitmq:3.13-alpine");

    @DynamicPropertySource
    static void rabbitProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.rabbitmq.host", rabbit::getHost);
        registry.add("spring.rabbitmq.port", rabbit::getAmqpPort);
        registry.add("spring.rabbitmq.username", rabbit::getAdminUsername);
        registry.add("spring.rabbitmq.password", rabbit::getAdminPassword);
    }

    @Autowired
    R2dbcOutboxStore outboxStore;

    @Autowired
    ReactiveReliablePublisher publisher;

    @Autowired
    R2dbcOutboxProperties outboxProperties;

    @Autowired
    DatabaseClient databaseClient;

    @Autowired
    OutboxListenerProbe probe;

    @Autowired
    SampleReactiveIdempotencyStore idempotencyStore;

    @Test
    void r2dbcOutboxFlushPublishesThroughRabbitAndListenerConsumesThenMarksPublished() throws Exception {
        String id = UUID.randomUUID().toString();
        OutboxMessage message = new OutboxMessage(
                id,
                "order.outbox",
                "outbox-order-1",
                "outbox-order-1-key",
                null,
                new OrderCreated("outbox-order-1"),
                Map.of("x-correlation-id", "outbox-order-1-correlation"),
                MessageStatus.PENDING,
                0,
                null,
                Instant.now(),
                null,
                null
        );

        int invocationsBefore = probe.invocations();

        Integer flushed = outboxStore.initializeSchema()
                .then(outboxStore.save(message))
                .then(new ReactiveOutboxFlushScheduler(outboxStore, publisher, outboxProperties, Clock.systemUTC()).flushBatch())
                .block(Duration.ofSeconds(20));

        assertEquals(1, flushed);
        assertTrue(probe.awaitInvocations(invocationsBefore + 1, Duration.ofSeconds(15)));
        assertEquals(invocationsBefore + 1, probe.invocations());
        assertEquals("outbox-order-1", probe.lastMessage().payload().orderId());
        assertEquals("outbox-order-1-key", probe.lastMessage().idempotencyKey());
        assertEquals("outbox-order-1-correlation", probe.lastMessage().correlationId());
        assertEquals(MessageStatus.PUBLISHED.name(), status(id));
    }

    @Test
    void replayAfterOutboxDeliveryIsSkippedByIdempotency() throws Exception {
        String idempotencyKey = "outbox-order-replay-key";
        String id = UUID.randomUUID().toString();
        OutboxMessage message = new OutboxMessage(
                id,
                "order.outbox",
                "outbox-order-replay",
                idempotencyKey,
                null,
                new OrderCreated("outbox-order-replay"),
                Map.of("x-correlation-id", "outbox-order-replay-correlation"),
                MessageStatus.PENDING,
                0,
                null,
                Instant.now(),
                null,
                null
        );

        int invocationsBefore = probe.invocations();

        Integer flushed = outboxStore.initializeSchema()
                .then(outboxStore.save(message))
                .then(new ReactiveOutboxFlushScheduler(outboxStore, publisher, outboxProperties, Clock.systemUTC()).flushBatch())
                .block(Duration.ofSeconds(20));

        assertEquals(1, flushed);
        assertTrue(probe.awaitInvocations(invocationsBefore + 1, Duration.ofSeconds(15)));
        assertEquals(invocationsBefore + 1, probe.invocations());
        assertTrue(idempotencyStore.awaitTryStarts(idempotencyKey, 1, Duration.ofSeconds(5)));

        publisher.publish(
                "order.outbox",
                new OrderCreated("outbox-order-replay"),
                PublishOptions.builder()
                        .idempotencyKey(idempotencyKey)
                        .correlationId("outbox-order-replay-correlation")
                        .build()
        ).block(Duration.ofSeconds(10));

        assertTrue(idempotencyStore.awaitTryStarts(idempotencyKey, 2, Duration.ofSeconds(10)));
        assertFalse(probe.awaitInvocations(invocationsBefore + 2, Duration.ofMillis(250)));
        assertEquals(invocationsBefore + 1, probe.invocations());
        assertEquals(MessageStatus.PUBLISHED.name(), status(id));
    }

    private String status(String id) {
        return databaseClient.sql("select status from message_outbox where id = :id")
                .bind("id", id)
                .map((row, metadata) -> row.get("status", String.class))
                .one()
                .block(Duration.ofSeconds(5));
    }

    @SpringBootConfiguration
    @EnableAutoConfiguration
    static class SampleApplication {

        public static void main(String[] args) {
            SpringApplication.run(SampleApplication.class, args);
        }

        @Bean
        OutboxListenerProbe outboxListenerProbe() {
            return new OutboxListenerProbe();
        }

        @Bean
        SampleOutboxListener sampleOutboxListener(OutboxListenerProbe probe) {
            return new SampleOutboxListener(probe);
        }

        @Bean
        SampleReactiveIdempotencyStore sampleReactiveIdempotencyStore() {
            return new SampleReactiveIdempotencyStore();
        }

        @Bean
        MessageSerializer reliableMessageSerializer(ObjectMapper objectMapper) {
            return new TestReliableMessageSerializer(objectMapper);
        }

        @Bean
        MeterRegistry meterRegistry() {
            return new SimpleMeterRegistry();
        }

        @Bean
        io.r2dbc.spi.ConnectionFactory r2dbcConnectionFactory() {
            return ConnectionFactories.get("r2dbc:h2:mem:///" + UUID.randomUUID() + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1");
        }

        @Bean
        ConnectionFactory connectionFactory(Environment environment) {
            CachingConnectionFactory connectionFactory = new CachingConnectionFactory(
                    environment.getRequiredProperty("spring.rabbitmq.host"),
                    Integer.parseInt(environment.getRequiredProperty("spring.rabbitmq.port"))
            );
            connectionFactory.setUsername(environment.getRequiredProperty("spring.rabbitmq.username"));
            connectionFactory.setPassword(environment.getRequiredProperty("spring.rabbitmq.password"));
            return connectionFactory;
        }

        @Bean
        RabbitAdmin rabbitAdmin(ConnectionFactory connectionFactory) {
            return new RabbitAdmin(connectionFactory);
        }

        @Bean
        @Primary
        RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory) {
            RabbitTemplate rabbitTemplate = new RabbitTemplate(connectionFactory);
            rabbitTemplate.afterPropertiesSet();
            return rabbitTemplate;
        }
    }

    static class SampleOutboxListener {
        private final OutboxListenerProbe probe;

        SampleOutboxListener(OutboxListenerProbe probe) {
            this.probe = probe;
        }

        @ReactiveReliableListener("order.outbox")
        Mono<Void> onOrderOutbox(ReliableMessage<OrderCreated> message) {
            return Mono.fromRunnable(() -> probe.record(message));
        }
    }

    static final class OutboxListenerProbe {
        private final CountDownLatch latch = new CountDownLatch(1);
        private final AtomicInteger invocations = new AtomicInteger();
        private final AtomicReference<ReliableMessage<OrderCreated>> lastMessage = new AtomicReference<>();

        void record(ReliableMessage<OrderCreated> message) {
            invocations.incrementAndGet();
            lastMessage.set(message);
            latch.countDown();
        }

        boolean await(Duration timeout) throws InterruptedException {
            return latch.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
        }

        boolean awaitInvocations(int expected, Duration timeout) throws InterruptedException {
            long deadline = System.nanoTime() + timeout.toNanos();
            while (invocations() < expected && System.nanoTime() < deadline) {
                Thread.sleep(10);
            }
            return invocations() >= expected;
        }

        int invocations() {
            return invocations.get();
        }

        ReliableMessage<OrderCreated> lastMessage() {
            return lastMessage.get();
        }
    }

    static final class SampleReactiveIdempotencyStore implements ReactiveIdempotencyStore {
        private final ConcurrentMap<String, Boolean> successes = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, AtomicInteger> tryStarts = new ConcurrentHashMap<>();

        @Override
        public Mono<IdempotencyStartResult> tryStart(String key, Duration ttl) {
            return Mono.fromSupplier(() -> {
                tryStarts.computeIfAbsent(key, ignored -> new AtomicInteger()).incrementAndGet();
                return successes.containsKey(key)
                        ? IdempotencyStartResult.duplicate(io.github.huynhngochuyhoang.reliablemessage.webflux.IdempotencyState.SUCCESS)
                    : IdempotencyStartResult.startAccepted();
            });
        }

        @Override
        public Mono<Void> markSuccess(String key) {
            return Mono.fromRunnable(() -> successes.put(key, true));
        }

        @Override
        public Mono<Void> markFailed(String key, Throwable error) {
            return Mono.empty();
        }

        boolean awaitTryStarts(String key, int expected, Duration timeout) throws InterruptedException {
            long deadline = System.nanoTime() + timeout.toNanos();
            while (tryStartCount(key) < expected && System.nanoTime() < deadline) {
                Thread.sleep(10);
            }
            return tryStartCount(key) >= expected;
        }

        int tryStartCount(String key) {
            AtomicInteger count = tryStarts.get(key);
            return count == null ? 0 : count.get();
        }
    }

    static final class TestReliableMessageSerializer implements MessageSerializer {
        private static final TypeReference<Map<String, String>> STRING_MAP = new TypeReference<>() {
        };

        private final ObjectMapper objectMapper;

        TestReliableMessageSerializer(ObjectMapper objectMapper) {
            this.objectMapper = objectMapper.findAndRegisterModules();
        }

        @Override
        public <T> byte[] serialize(ReliableMessage<T> message) {
            try {
                return objectMapper.writeValueAsBytes(message);
            } catch (Exception error) {
                throw new IllegalStateException("Unable to serialize reliable message", error);
            }
        }

        @Override
        public <T> ReliableMessage<T> deserialize(byte[] content, Class<T> payloadType) {
            try {
                JsonNode root = objectMapper.readTree(content);
                T payload = objectMapper.treeToValue(root.get("payload"), payloadType);
                Map<String, String> headers = objectMapper.convertValue(root.get("headers"), STRING_MAP);
                return new ReliableMessage<>(
                        root.get("messageId").asText(),
                        root.get("eventName").asText(),
                        textOrNull(root.get("aggregateId")),
                        textOrNull(root.get("idempotencyKey")),
                        textOrNull(root.get("correlationId")),
                        textOrNull(root.get("traceId")),
                        Instant.parse(root.get("occurredAt").asText()),
                        headers,
                        payload
                );
            } catch (Exception error) {
                throw new IllegalStateException("Unable to deserialize reliable message", error);
            }
        }

        private static String textOrNull(JsonNode node) {
            return node == null || node.isNull() ? null : node.asText();
        }
    }

    record OrderCreated(String orderId) {
    }
}
