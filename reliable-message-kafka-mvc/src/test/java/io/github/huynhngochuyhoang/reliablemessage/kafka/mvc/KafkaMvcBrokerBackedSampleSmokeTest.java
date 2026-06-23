package io.github.huynhngochuyhoang.reliablemessage.kafka.mvc;

import io.github.huynhngochuyhoang.reliablemessage.core.PublishOptions;
import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessage;
import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessageHeaders;
import io.github.huynhngochuyhoang.reliablemessage.mvc.*;
import io.github.huynhngochuyhoang.reliablemessage.outbox.jdbc.OutboxFlushScheduler;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(
        classes = KafkaMvcBrokerBackedSampleSmokeTest.SampleApplication.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "message.reliability.transport=kafka",
                "message.reliability.service-name=s8-orders",
                "message.reliability.kafka.topic-prefix=s8.mvc.kafka.",
                "message.reliability.kafka.consumer-group=s8-mvc-kafka",
                "message.reliability.kafka.partitions=1",
                "message.reliability.kafka.replication-factor=1",
                "message.reliability.retry.attempts=2",
                "message.reliability.retry.backoff[0]=100ms",
                "message.reliability.outbox.flush-enabled=false",
                "spring.kafka.consumer.auto-offset-reset=earliest",
                "spring.kafka.consumer.enable-auto-commit=false",
                "spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer",
                "spring.kafka.consumer.value-deserializer=org.apache.kafka.common.serialization.ByteArrayDeserializer",
                "spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer",
                "spring.kafka.producer.value-serializer=org.apache.kafka.common.serialization.ByteArraySerializer",
                "spring.datasource.url=jdbc:h2:mem:s8-mvc-kafka;MODE=PostgreSQL;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=false",
                "spring.datasource.username=sa",
                "spring.datasource.password="
        }
)
class KafkaMvcBrokerBackedSampleSmokeTest {

    private static final Duration WAIT = Duration.ofSeconds(20);
    private static final String TOPIC_PREFIX = "s8.mvc.kafka.";

    @Container
    static final KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("apache/kafka-native:3.8.0")
    );

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate restTemplate;

    @Autowired
    ReliablePublisher reliablePublisher;

    @Autowired
    KafkaTemplate<String, byte[]> kafkaTemplate;

    @Autowired
    OutboxPublisher outboxPublisher;

    @Autowired
    OutboxStore outboxStore;

    @Autowired
    OutboxFlushScheduler outboxFlushScheduler;

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Autowired
    SampleInvocationProbe probe;

    @Autowired
    SampleIdempotencyStore idempotencyStore;

    @Autowired
    MeterRegistry meterRegistry;

    @Autowired
    ConfigurableApplicationContext context;

    @Test
    void httpEndpointPublishesEnvelopeMetadataAndKafkaHeaders() throws Exception {
        ResponseEntity<Void> response = restTemplate.postForEntity(
                "http://localhost:" + port + "/orders/order-1/events",
                null,
                Void.class
        );

        assertEquals(HttpStatus.ACCEPTED, response.getStatusCode());
        await(() -> probe.published.invocations() == 1, WAIT);
        ReliableMessage<OrderCreated> handled = probe.published.lastMessage();
        assertNotNull(handled);
        assertEquals("order-1", handled.payload().orderId());
        assertEquals("order-1", handled.aggregateId());
        assertEquals("order-1-event", handled.idempotencyKey());
        assertEquals("order-1-correlation", handled.correlationId());
        assertEquals("sample", handled.headers().get("source"));

        ConsumerRecord<String, byte[]> record = consumeOne(TOPIC_PREFIX + "order.published");
        assertEquals("order-1", record.key());
        assertEquals("order-1-correlation", header(record, ReliableMessageHeaders.CORRELATION_ID));
        assertEquals("order.published", header(record, ReliableMessageHeaders.EVENT_NAME));
        assertEquals("sample", header(record, "source"));

        assertCounter("message_publish_total", "order.published", "success", 1.0);
        assertCounter("message_consume_total", "order.published", "success", 1.0);
    }

    @Test
    void newMessageTransitionsToSuccessAndDuplicateSuccessSkipsBusinessHandler() throws Exception {
        String key = "duplicate-success-key";
        reliablePublisher.publish(
                "order.idempotent",
                new OrderCreated("first"),
                PublishOptions.builder().idempotencyKey(key).partitionKey(key).build()
        );

        await(() -> idempotencyStore.state(key) == IdempotencyState.SUCCESS, WAIT);
        await(() -> probe.idempotent.invocations() == 1, WAIT);

        reliablePublisher.publish(
                "order.idempotent",
                new OrderCreated("replay"),
                PublishOptions.builder().idempotencyKey(key).partitionKey(key).build()
        );

        await(() -> idempotencyStore.tryStartCount(key) == 2, WAIT);
        assertRemains(() -> probe.idempotent.invocations() == 1, Duration.ofMillis(400));
        assertEquals(1, probe.idempotent.invocations());
        assertCounter("message_duplicate_total", "order.idempotent", "duplicate", 1.0);
    }

    @Test
    void duplicateProcessingAndFailedAreNotTreatedAsSuccess() throws Exception {
        assertDuplicateDoesNotInvoke("duplicate-processing-key", IdempotencyState.PROCESSING);
        assertDuplicateDoesNotInvoke("duplicate-failed-key", IdempotencyState.FAILED);
    }

    @Test
    void handlerFailureRoutesThroughRetryTopicAndEventuallySucceeds() throws Exception {
        reliablePublisher.publish("order.retry", new OrderCreated("retry-1"), PublishOptions.empty());

        await(() -> probe.retry.invocations() == 2, WAIT);
        assertEquals(2, probe.retry.invocations());
        assertCounter("message_retry_total", "order.retry", "retry", 1.0);
        assertCounterAtLeast("message_consume_failed_total", "order.retry", "failed", 1.0);
    }

    @Test
    void terminalHandlerFailureAndInvalidPayloadReachKafkaDlt() throws Exception {
        reliablePublisher.publish("order.dlt", new OrderCreated("dlt-1"), PublishOptions.empty());

        ConsumerRecord<String, byte[]> terminal = consumeOne(
                KafkaTopicNames.dltTopic(TOPIC_PREFIX + "order.dlt", "s8-mvc-kafka")
        );
        assertEquals("2", header(terminal, ReliableMessageHeaders.RETRY_COUNT));

        kafkaTemplate.send(TOPIC_PREFIX + "order.invalid", "invalid", "not-json".getBytes(StandardCharsets.UTF_8)).get();
        ConsumerRecord<String, byte[]> invalid = consumeOne(
                KafkaTopicNames.dltTopic(TOPIC_PREFIX + "order.invalid", "s8-mvc-kafka")
        );
        assertEquals("2", header(invalid, ReliableMessageHeaders.RETRY_COUNT));
        assertEquals(0, probe.invalid.invocations());
        assertCounterAtLeast("message_dlq_total", "order.invalid", "dlt", 1.0);
    }

    @Test
    void jdbcOutboxFlushPublishesThroughKafkaAndDoesNotRepublishAfterSuccess() throws Exception {
        outboxPublisher.publishLater(
                "order.outbox",
                new OrderCreated("outbox-1"),
                PublishOptions.builder()
                        .aggregateId("outbox-1")
                        .idempotencyKey("outbox-1-event")
                        .correlationId("outbox-1-correlation")
                        .partitionKey("outbox-1")
                        .build()
        );

        String outboxId = outboxStore.findForAdmin(10).stream()
                .filter(message -> "order.outbox".equals(message.eventName()))
                .findFirst()
                .orElseThrow()
                .id();
        assertEquals(1, outboxFlushScheduler.flushBatch());
        await(() -> probe.outbox.invocations() == 1, WAIT);

        assertEquals("PUBLISHED", jdbcTemplate.queryForObject(
                "select status from message_outbox where id = ?", String.class, outboxId));
        assertEquals(0, outboxFlushScheduler.flushBatch());
        assertRemains(() -> probe.outbox.invocations() == 1, Duration.ofMillis(400));
    }

    @Test
    void kafkaSampleUsesAutoConfiguredEventBeansWithoutRabbitOrRpc() {
        assertTrue(context.getBean(KafkaReliablePublisher.class) instanceof ReliablePublisher);
        assertNotNull(context.getBean(KafkaReliableListenerRegistrar.class));
        assertNotNull(context.getBean(KafkaRetryStrategy.class));
        assertNotNull(context.getBean(OutboxFlushScheduler.class));
        assertFalse(context.containsBean("reactiveRabbitBridgePublisher"));
        assertFalse(context.containsBean("reactiveRabbitRpcClient"));
        assertThrows(NoSuchBeanDefinitionException.class, () -> context.getBean("rabbitTemplate"));
        assertThrows(ClassNotFoundException.class,
                () -> Class.forName("org.springframework.amqp.rabbit.core.RabbitTemplate"));
        assertThrows(ClassNotFoundException.class,
                () -> Class.forName("org.springframework.amqp.rabbit.AsyncRabbitTemplate"));
    }

    private void assertDuplicateDoesNotInvoke(String key, IdempotencyState state) throws Exception {
        int before = probe.duplicates.invocations();
        idempotencyStore.setState(key, state);

        reliablePublisher.publish(
                "order.duplicate",
                new OrderCreated(key),
                PublishOptions.builder().idempotencyKey(key).partitionKey(key).build()
        );

        await(() -> idempotencyStore.tryStartCount(key) == 1, WAIT);
        assertRemains(() -> probe.duplicates.invocations() == before, Duration.ofMillis(400));
        assertEquals(state, idempotencyStore.state(key));
    }

    private ConsumerRecord<String, byte[]> consumeOne(String topic) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "s8-observer-" + UUID.randomUUID());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(List.of(topic));
            Instant deadline = Instant.now().plus(WAIT);
            while (Instant.now().isBefore(deadline)) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(200));
                if (!records.isEmpty()) {
                    return records.iterator().next();
                }
            }
        }
        throw new AssertionError("No Kafka record received from " + topic);
    }

    private void assertCounter(String name, String eventName, String status, double expected) {
        assertEquals(expected, meterRegistry.find(name)
                .tags("runtime", "mvc", "transport", "kafka", "event_name", eventName, "status", status)
                .counter().count());
    }

    private void assertCounterAtLeast(String name, String eventName, String status, double minimum) {
        assertTrue(meterRegistry.find(name)
                .tags("runtime", "mvc", "transport", "kafka", "event_name", eventName, "status", status)
                .counter().count() >= minimum);
    }

    private static String header(ConsumerRecord<String, byte[]> record, String name) {
        org.apache.kafka.common.header.Header header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    private static void await(BooleanSupplier condition, Duration timeout) throws InterruptedException {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(20);
        }
        assertTrue(condition.getAsBoolean(), "Condition was not met within " + timeout);
    }

    private static void assertRemains(BooleanSupplier condition, Duration duration) throws InterruptedException {
        Instant deadline = Instant.now().plus(duration);
        while (Instant.now().isBefore(deadline)) {
            assertTrue(condition.getAsBoolean());
            Thread.sleep(20);
        }
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
        SampleIdempotencyStore sampleIdempotencyStore() {
            return new SampleIdempotencyStore();
        }

        @Bean
        SampleOrderListener sampleOrderListener(SampleInvocationProbe probe) {
            return new SampleOrderListener(probe);
        }

        @Bean
        SampleOrderController sampleOrderController(ReliablePublisher publisher) {
            return new SampleOrderController(publisher);
        }
    }

    @RestController
    static final class SampleOrderController {
        private final ReliablePublisher publisher;

        SampleOrderController(ReliablePublisher publisher) {
            this.publisher = publisher;
        }

        @PostMapping("/orders/{orderId}/events")
        ResponseEntity<Void> publish(@PathVariable("orderId") String orderId) {
            publisher.publish(
                    "order.published",
                    new OrderCreated(orderId),
                    PublishOptions.builder()
                            .aggregateId(orderId)
                            .idempotencyKey(orderId + "-event")
                            .correlationId(orderId + "-correlation")
                            .partitionKey(orderId)
                            .header("source", "sample")
                            .build()
            );
            return ResponseEntity.accepted().build();
        }
    }

    static final class SampleOrderListener {
        private final SampleInvocationProbe probe;

        SampleOrderListener(SampleInvocationProbe probe) {
            this.probe = probe;
        }

        @ReliableListener("order.published")
        void onPublished(ReliableMessage<OrderCreated> message) {
            probe.published.record(message);
        }

        @ReliableListener("order.idempotent")
        void onIdempotent(ReliableMessage<OrderCreated> message) {
            probe.idempotent.record(message);
        }

        @ReliableListener("order.duplicate")
        void onDuplicate(ReliableMessage<OrderCreated> message) {
            probe.duplicates.record(message);
        }

        @ReliableListener("order.retry")
        void onRetry(ReliableMessage<OrderCreated> message) {
            int attempt = probe.retry.record(message);
            if (attempt == 1) {
                throw new IllegalStateException("retry once");
            }
        }

        @ReliableListener("order.dlt")
        void onDlt(ReliableMessage<OrderCreated> message) {
            throw new IllegalStateException("always fail");
        }

        @ReliableListener("order.invalid")
        void onInvalid(ReliableMessage<OrderCreated> message) {
            probe.invalid.record(message);
        }

        @ReliableListener("order.outbox")
        void onOutbox(ReliableMessage<OrderCreated> message) {
            probe.outbox.record(message);
        }
    }

    record OrderCreated(String orderId) {
    }

    static final class SampleInvocationProbe {
        final EventProbe published = new EventProbe();
        final EventProbe idempotent = new EventProbe();
        final EventProbe duplicates = new EventProbe();
        final EventProbe retry = new EventProbe();
        final EventProbe invalid = new EventProbe();
        final EventProbe outbox = new EventProbe();
    }

    static final class EventProbe {
        private final AtomicInteger invocations = new AtomicInteger();
        private final AtomicReference<ReliableMessage<OrderCreated>> lastMessage = new AtomicReference<>();

        int record(ReliableMessage<OrderCreated> message) {
            lastMessage.set(message);
            return invocations.incrementAndGet();
        }

        int invocations() {
            return invocations.get();
        }

        ReliableMessage<OrderCreated> lastMessage() {
            return lastMessage.get();
        }
    }

    static final class SampleIdempotencyStore implements IdempotencyStore {
        private final ConcurrentMap<String, IdempotencyState> states = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, AtomicInteger> tryStarts = new ConcurrentHashMap<>();

        @Override
        public IdempotencyStartResult tryStart(String key, Duration ttl) {
            tryStarts.computeIfAbsent(key, ignored -> new AtomicInteger()).incrementAndGet();
            IdempotencyState existing = states.putIfAbsent(key, IdempotencyState.PROCESSING);
            return existing == null
                    ? IdempotencyStartResult.startAccepted()
                    : IdempotencyStartResult.duplicate(existing);
        }

        @Override
        public void markSuccess(String key) {
            states.put(key, IdempotencyState.SUCCESS);
        }

        @Override
        public void markFailed(String key, Throwable error) {
            states.put(key, IdempotencyState.FAILED);
        }

        void setState(String key, IdempotencyState state) {
            states.put(key, state);
        }

        IdempotencyState state(String key) {
            return states.get(key);
        }

        int tryStartCount(String key) {
            AtomicInteger count = tryStarts.get(key);
            return count == null ? 0 : count.get();
        }
    }
}
