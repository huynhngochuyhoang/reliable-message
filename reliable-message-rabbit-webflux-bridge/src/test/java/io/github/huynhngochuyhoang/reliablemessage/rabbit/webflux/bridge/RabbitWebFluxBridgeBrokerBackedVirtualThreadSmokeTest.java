package io.github.huynhngochuyhoang.reliablemessage.rabbit.webflux.bridge;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.huynhngochuyhoang.reliablemessage.core.PublishOptions;
import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessage;
import io.github.huynhngochuyhoang.reliablemessage.core.serialization.MessageSerializer;
import io.github.huynhngochuyhoang.reliablemessage.webflux.*;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.rabbit.AsyncRabbitTemplate;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.core.env.Environment;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(
        classes = RabbitWebFluxBridgeBrokerBackedVirtualThreadSmokeTest.SampleApplication.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "message.reliability.transport=rabbit",
                "message.reliability.service-name=webflux-orders",
                "message.reliability.rabbit.exchange=s3.webflux.rabbit.virtual.events",
                "message.reliability.rabbit.bridge.executor-mode=virtual-thread",
                "message.reliability.rabbit.bridge.max-concurrency=4",
                "spring.autoconfigure.exclude=org.springframework.boot.autoconfigure.r2dbc.R2dbcAutoConfiguration,io.github.huynhngochuyhoang.reliablemessage.outbox.r2dbc.R2dbcOutboxAutoConfiguration"
        }
)
class RabbitWebFluxBridgeBrokerBackedVirtualThreadSmokeTest {

    @Container
    static final RabbitMQContainer rabbit = new RabbitMQContainer("rabbitmq:3.13-alpine");

    @DynamicPropertySource
    static void rabbitProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.rabbitmq.host", rabbit::getHost);
        registry.add("spring.rabbitmq.port", rabbit::getAmqpPort);
        registry.add("spring.rabbitmq.username", rabbit::getAdminUsername);
        registry.add("spring.rabbitmq.password", rabbit::getAdminPassword);
    }

    @LocalServerPort
    int port;

    @Autowired
    ReactiveReliablePublisher publisher;

    @Autowired
    RecordingRabbitTemplate rabbitTemplate;

    @Autowired
    SampleInvocationProbe probe;

    @Autowired
    SampleReactiveIdempotencyStore idempotencyStore;

    @Autowired
    RecordingFailureHandler failureHandler;

    @Autowired
    MeterRegistry meterRegistry;

    @Autowired
    ConfigurableApplicationContext context;

    @Test
    void webFluxEndpointPublishesRabbitEventAndListenerProcessesItOnce() throws Exception {
        WebTestClient.bindToServer()
                .baseUrl("http://localhost:" + port)
                .build()
                .post()
                .uri("/orders/{orderId}/events", "order-1")
                .exchange()
                .expectStatus().isAccepted();

        assertTrue(probe.orders.await(Duration.ofSeconds(10)));
        assertEquals(1, probe.orders.invocations());
        assertEquals("order-1", probe.orders.lastOrderId());
        assertEquals("order-1-event", probe.orders.lastMessage().idempotencyKey());
        assertEquals("order-1-correlation", probe.orders.lastMessage().correlationId());
        assertNotNull(probe.controllerThread());
        assertTrue(probe.controllerThread().contains("http"), probe.controllerThread());
        assertTrue(rabbitTemplate.awaitSend(Duration.ofSeconds(5)));
        assertTrue(rabbitTemplate.lastSendThread().startsWith("reliable-message-rabbit-bridge-virtual-"));
        assertNotEquals(probe.controllerThread(), rabbitTemplate.lastSendThread());
    }

    @Test
    void sampleUsesWebFluxRabbitEventBridgeBeansOnly() {
        assertTrue(context.containsBean("reactiveRabbitBridgePublisher"));
        assertTrue(context.containsBean("reactiveRabbitBridgeListenerRegistrar"));
        assertTrue(context.containsBean("reactiveRabbitBridgeTopologyAutoConfigurer"));
        assertTrue(context.containsBean("rabbitBridgeExecutorProvider"));
        assertFalse(context.containsBean("reactiveRabbitRpcClient"));
        assertThrows(NoSuchBeanDefinitionException.class, () -> context.getBean(AsyncRabbitTemplate.class));
    }


    @SpringBootConfiguration
    @EnableAutoConfiguration
    static class SampleApplication {

        public static void main(String[] args) {
            SpringApplication.run(SampleApplication.class, args);
        }

        @Bean
        SampleInvocationProbe sampleInvocationProbe() {
            return new SampleInvocationProbe();
        }

        @Bean
        SampleReactiveIdempotencyStore sampleReactiveIdempotencyStore() {
            return new SampleReactiveIdempotencyStore();
        }

        @Bean
        RecordingFailureHandler recordingFailureHandler() {
            return new RecordingFailureHandler();
        }

        @Bean
        SampleOrderListener sampleOrderListener(SampleInvocationProbe probe) {
            return new SampleOrderListener(probe);
        }

        @Bean
        SampleOrderController sampleOrderController(ReactiveReliablePublisher publisher, SampleInvocationProbe probe) {
            return new SampleOrderController(publisher, probe);
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
        RecordingRabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory) {
            RecordingRabbitTemplate rabbitTemplate = new RecordingRabbitTemplate(connectionFactory);
            rabbitTemplate.afterPropertiesSet();
            return rabbitTemplate;
        }
    }

    @RestController
    static class SampleOrderController {
        private final ReactiveReliablePublisher publisher;
        private final SampleInvocationProbe probe;

        SampleOrderController(ReactiveReliablePublisher publisher, SampleInvocationProbe probe) {
            this.publisher = publisher;
            this.probe = probe;
        }

        @PostMapping("/orders/{orderId}/events")
        Mono<ResponseEntity<Void>> publishOrderCreated(@PathVariable("orderId") String orderId) {
            probe.recordControllerThread(Thread.currentThread().getName());
            return publisher.publish(
                            "order.created",
                            new OrderCreated(orderId),
                            PublishOptions.builder()
                                    .aggregateId(orderId)
                                    .idempotencyKey(orderId + "-event")
                                    .correlationId(orderId + "-correlation")
                                    .build()
                    )
                    .thenReturn(ResponseEntity.accepted().build());
        }
    }

    static class SampleOrderListener {
        private final SampleInvocationProbe probe;

        SampleOrderListener(SampleInvocationProbe probe) {
            this.probe = probe;
        }

        @ReactiveReliableListener("order.created")
        Mono<Void> onOrderCreated(ReliableMessage<OrderCreated> message) {
            return Mono.fromRunnable(() -> probe.orders.record(message));
        }

        @ReactiveReliableListener("order.duplicate")
        Mono<Void> onDuplicate(ReliableMessage<OrderCreated> message) {
            return Mono.fromRunnable(() -> probe.duplicates.record(message));
        }

        @ReactiveReliableListener("order.retry")
        Mono<Void> onRetry(ReliableMessage<OrderCreated> message) {
            return Mono.fromRunnable(() -> {
                probe.retry.record(message);
                if (probe.retry.invocations() == 1) {
                    throw new IllegalStateException("retry once");
                }
            });
        }

        @ReactiveReliableListener("order.metrics")
        Mono<Void> onMetrics(ReliableMessage<OrderCreated> message) {
            return Mono.fromRunnable(() -> probe.metrics.record(message));
        }
    }

    static final class RecordingRabbitTemplate extends RabbitTemplate {
        private final AtomicReference<String> lastSendThread = new AtomicReference<>();
        private final CountDownLatch sendLatch = new CountDownLatch(1);

        RecordingRabbitTemplate(ConnectionFactory connectionFactory) {
            super(connectionFactory);
        }

        @Override
        public void convertAndSend(String exchange, String routingKey, Object object) {
            lastSendThread.set(Thread.currentThread().getName());
            sendLatch.countDown();
            super.convertAndSend(exchange, routingKey, object);
        }

        boolean awaitSend(Duration timeout) throws InterruptedException {
            return sendLatch.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
        }

        String lastSendThread() {
            return lastSendThread.get();
        }
    }

    static final class SampleInvocationProbe {
        final EventProbe orders = new EventProbe();
        final EventProbe duplicates = new EventProbe();
        final EventProbe retry = new EventProbe(2);
        final EventProbe metrics = new EventProbe();
        private final AtomicReference<String> controllerThread = new AtomicReference<>();

        void recordControllerThread(String threadName) {
            controllerThread.set(threadName);
        }

        String controllerThread() {
            return controllerThread.get();
        }
    }

    static final class EventProbe {
        private final CountDownLatch latch;
        private final AtomicInteger invocations = new AtomicInteger();
        private final AtomicReference<ReliableMessage<OrderCreated>> lastMessage = new AtomicReference<>();

        EventProbe() {
            this(1);
        }

        EventProbe(int expectedInvocations) {
            this.latch = new CountDownLatch(expectedInvocations);
        }

        void record(ReliableMessage<OrderCreated> message) {
            invocations.incrementAndGet();
            lastMessage.set(message);
            latch.countDown();
        }

        boolean await(Duration timeout) throws InterruptedException {
            return latch.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
        }

        int invocations() {
            return invocations.get();
        }

        String lastOrderId() {
            ReliableMessage<OrderCreated> message = lastMessage();
            return message == null ? null : message.payload().orderId();
        }

        ReliableMessage<OrderCreated> lastMessage() {
            return lastMessage.get();
        }
    }

    static final class SampleReactiveIdempotencyStore implements ReactiveIdempotencyStore {
        private final ConcurrentMap<String, IdempotencyState> duplicates = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, AtomicInteger> tryStarts = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, CountDownLatch> firstTryStartLatches = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, CountDownLatch> secondTryStartLatches = new ConcurrentHashMap<>();

        @Override
        public Mono<IdempotencyStartResult> tryStart(String key, Duration ttl) {
            return Mono.fromSupplier(() -> {
                int count = tryStarts.computeIfAbsent(key, ignored -> new AtomicInteger()).incrementAndGet();
                firstTryStartLatches.computeIfAbsent(key, ignored -> new CountDownLatch(1)).countDown();
                if (count >= 2) {
                    secondTryStartLatches.computeIfAbsent(key, ignored -> new CountDownLatch(1)).countDown();
                }
                IdempotencyState duplicate = duplicates.get(key);
                return duplicate == null ? IdempotencyStartResult.startAccepted() : IdempotencyStartResult.duplicate(duplicate);
            });
        }

        @Override
        public Mono<Void> markSuccess(String key) {
            return Mono.fromRunnable(() -> duplicates.put(key, IdempotencyState.SUCCESS));
        }

        @Override
        public Mono<Void> markFailed(String key, Throwable error) {
            return Mono.fromRunnable(() -> duplicates.put(key, IdempotencyState.FAILED));
        }

        boolean awaitTryStart(String key, Duration timeout) throws InterruptedException {
            return firstTryStartLatches.computeIfAbsent(key, ignored -> new CountDownLatch(1))
                    .await(timeout.toMillis(), TimeUnit.MILLISECONDS);
        }

        boolean awaitTryStartCount(String key, int expectedCount, Duration timeout) throws InterruptedException {
            if (tryStartCount(key) >= expectedCount) {
                return true;
            }
            if (expectedCount != 2) {
                throw new IllegalArgumentException("Only a second tryStart wait is supported by this test fixture");
            }
            return secondTryStartLatches.computeIfAbsent(key, ignored -> new CountDownLatch(1))
                    .await(timeout.toMillis(), TimeUnit.MILLISECONDS);
        }

        int tryStartCount(String key) {
            AtomicInteger count = tryStarts.get(key);
            return count == null ? 0 : count.get();
        }
    }

    static final class RecordingFailureHandler implements ReactiveRabbitBridgeFailureOutcomeHandler {
        private final CountDownLatch failureLatch = new CountDownLatch(1);
        private final AtomicInteger failures = new AtomicInteger();

        @Override
        public ReactiveRabbitBridgeFailureOutcome handleFailureWithOutcome(
                ReactiveRabbitBridgeListenerEndpoint endpoint,
                ReliableMessage<?> reliableMessage,
                org.springframework.amqp.core.Message amqpMessage,
                Throwable error
        ) {
            failures.incrementAndGet();
            failureLatch.countDown();
            return ReactiveRabbitBridgeFailureOutcome.RETRY;
        }

        boolean await(Duration timeout) throws InterruptedException {
            return failureLatch.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
        }

        boolean awaitFailures(int expectedCount, Duration timeout) throws InterruptedException {
            long deadline = System.nanoTime() + timeout.toNanos();
            while (failures() < expectedCount && System.nanoTime() < deadline) {
                Thread.sleep(10);
            }
            return failures() >= expectedCount;
        }

        int failures() {
            return failures.get();
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
