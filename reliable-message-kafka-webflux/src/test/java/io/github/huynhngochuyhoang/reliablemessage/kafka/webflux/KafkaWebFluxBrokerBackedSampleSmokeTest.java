package io.github.huynhngochuyhoang.reliablemessage.kafka.webflux;

import io.github.huynhngochuyhoang.reliablemessage.core.MessageStatus;
import io.github.huynhngochuyhoang.reliablemessage.core.PublishOptions;
import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessage;
import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessageHeaders;
import io.github.huynhngochuyhoang.reliablemessage.outbox.r2dbc.R2dbcOutboxProperties;
import io.github.huynhngochuyhoang.reliablemessage.outbox.r2dbc.R2dbcOutboxStore;
import io.github.huynhngochuyhoang.reliablemessage.outbox.r2dbc.ReactiveOutboxFlushScheduler;
import io.github.huynhngochuyhoang.reliablemessage.webflux.*;
import io.micrometer.core.instrument.MeterRegistry;
import io.r2dbc.spi.ConnectionFactories;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpStatus;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import reactor.core.publisher.Mono;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(
        classes = KafkaWebFluxBrokerBackedSampleSmokeTest.SampleApplication.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "message.reliability.runtime=webflux",
                "message.reliability.transport=kafka",
                "message.reliability.service-name=s9-orders",
                "message.reliability.kafka.topic-prefix=s9.webflux.kafka.",
                "message.reliability.kafka.consumer-group=s9-webflux-kafka",
                "message.reliability.kafka.listener-auto-startup=true",
                "message.reliability.reactive.max-concurrency=2",
                "message.reliability.reactive.prefetch=4",
                "message.reliability.retry.attempts=2",
                "message.reliability.retry.backoff[0]=100ms",
                "message.reliability.outbox.enabled=true",
                "message.reliability.outbox.flush-enabled=false",
                "message.reliability.outbox.batch-size=4",
                "message.reliability.outbox.publish-timeout=10s",
                "message.reliability.outbox.schema.payload-bytes-column-type=bytea"
        }
)
class KafkaWebFluxBrokerBackedSampleSmokeTest {

    private static final Duration WAIT = Duration.ofSeconds(20);

    @Container
    static final KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("apache/kafka-native:3.8.0")
    );

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add(
                "message.reliability.kafka.producer-properties[bootstrap.servers]",
                kafka::getBootstrapServers
        );
        registry.add(
                "message.reliability.kafka.consumer-properties[bootstrap.servers]",
                kafka::getBootstrapServers
        );
        registry.add(
                "message.reliability.kafka.consumer-properties[auto.offset.reset]",
                () -> "earliest"
        );
    }

    @LocalServerPort
    int port;

    @Autowired
    ReactiveReliablePublisher publisher;

    @Autowired
    ReactiveKafkaReliableListenerRegistrar registrar;

    @Autowired
    R2dbcOutboxStore outboxStore;

    @Autowired
    R2dbcOutboxProperties outboxProperties;

    @Autowired
    DatabaseClient databaseClient;

    @Autowired
    SampleProbe probe;

    @Autowired
    SampleReactiveIdempotencyStore idempotencyStore;

    @Autowired
    MeterRegistry meterRegistry;

    @Autowired
    ConfigurableApplicationContext context;

    @Test
    void reactiveHttpPublishCompletesAndPropagatesMetadataWithoutBlockingCaller() throws Exception {
        WebTestClient.bindToServer()
                .baseUrl("http://localhost:" + port)
                .build()
                .post()
                .uri("/orders/{orderId}/events", "order-1")
                .exchange()
                .expectStatus().isEqualTo(HttpStatus.ACCEPTED);

        assertTrue(probe.awaitInvocations("order.published", 1, WAIT));
        ReliableMessage<OrderCreated> message = probe.lastMessage("order.published");
        assertNotNull(message);
        assertEquals("order-1", message.payload().orderId());
        assertEquals("order-1", message.aggregateId());
        assertEquals("order-1-event", message.idempotencyKey());
        assertEquals("order-1-correlation", message.correlationId());
        assertEquals("sample", message.headers().get("source"));
        assertEquals("order-1", message.headers().get(ReliableMessageHeaders.PARTITION_KEY));
        assertTrue(probe.endpointThread().startsWith("webflux-http-"), probe.endpointThread());
        assertNotEquals(probe.endpointThread(), probe.listenerThread("order.published"));
        assertTrue(probe.listenerThread("order.published").startsWith("boundedElastic-"),
                probe.listenerThread("order.published"));

        assertCounter("message_publish_total", "order.published", "success", 1.0);
        awaitCounterAtLeast("message_consume_total", "order.published", "success", 1.0);
    }

    @Test
    void duplicateSuccessIsSkippedAndDuplicateInProgressStatesNeverRunBusinessLogic() throws Exception {
        String successKey = "s9-duplicate-success";
        publisher.publish(
                "order.idempotent",
                new OrderCreated("first"),
                PublishOptions.builder().idempotencyKey(successKey).partitionKey(successKey).build()
        ).block(WAIT);

        assertTrue(idempotencyStore.awaitState(successKey, IdempotencyState.SUCCESS, WAIT));
        assertTrue(probe.awaitInvocations("order.idempotent", 1, WAIT));

        publisher.publish(
                "order.idempotent",
                new OrderCreated("replay"),
                PublishOptions.builder().idempotencyKey(successKey).partitionKey(successKey).build()
        ).block(WAIT);

        assertTrue(idempotencyStore.awaitTryStarts(successKey, 2, WAIT));
        assertRemains(() -> probe.invocations("order.idempotent") == 1, Duration.ofMillis(400));
        assertCounter("message_duplicate_total", "order.idempotent", "duplicate", 1.0);

        assertDuplicateDoesNotInvoke("s9-processing", IdempotencyState.PROCESSING);
        assertDuplicateDoesNotInvoke("s9-failed", IdempotencyState.FAILED);
    }

    @Test
    void transientReactiveHandlerFailureIsRetriedAndEventuallySucceeds() throws Exception {
        publisher.publish("order.retry", new OrderCreated("retry-1"), PublishOptions.empty()).block(WAIT);

        assertTrue(probe.awaitInvocations("order.retry", 2, WAIT));
        assertEquals(2, probe.invocations("order.retry"));
        awaitCounterAtLeast("message_consume_failed_total", "order.retry", "failed", 1.0);
        awaitCounterAtLeast("message_consume_total", "order.retry", "success", 1.0);
    }

    @Test
    void r2dbcOutboxFlushPublishesThenDoesNotRepublishAfterSuccess() throws Exception {
        String id = UUID.randomUUID().toString();
        OutboxMessage outboxMessage = new OutboxMessage(
                id,
                "order.outbox",
                "outbox-1",
                "outbox-1-key",
                "outbox-1",
                new OrderCreated("outbox-1"),
                Map.of(ReliableMessageHeaders.CORRELATION_ID, "outbox-1-correlation"),
                MessageStatus.PENDING,
                0,
                null,
                Instant.now(),
                null,
                null
        );
        ReactiveOutboxFlushScheduler flusher = new ReactiveOutboxFlushScheduler(
                outboxStore, publisher, outboxProperties, Clock.systemUTC()
        );

        Integer flushed = outboxStore.initializeSchema()
                .then(outboxStore.save(outboxMessage))
                .then(flusher.flushBatch())
                .block(WAIT);

        assertEquals(1, flushed);
        assertTrue(probe.awaitInvocations("order.outbox", 1, WAIT));
        assertEquals("PUBLISHED", status(id));
        assertEquals(0, flusher.flushBatch().block(WAIT));
        assertRemains(() -> probe.invocations("order.outbox") == 1, Duration.ofMillis(400));
    }

    @Test
    void kafkaWebFluxUsesAutoConfiguredReactiveBeansWithoutRabbitOrRpc() {
        assertInstanceOf(ReactiveKafkaReliablePublisher.class, publisher);
        assertFalse(registrar.containers().isEmpty());
        assertNotNull(context.getBean(ReactiveKafkaRetryStrategy.class));
        assertNotNull(context.getBean(R2dbcOutboxStore.class));
        assertFalse(context.containsBean("reactiveRabbitBridgePublisher"));
        assertFalse(context.containsBean("reactiveRabbitRpcClient"));
        assertThrows(NoSuchBeanDefinitionException.class, () -> context.getBean("rabbitTemplate"));
        assertThrows(ClassNotFoundException.class,
                () -> Class.forName("org.springframework.amqp.rabbit.core.RabbitTemplate"));
        assertThrows(ClassNotFoundException.class,
                () -> Class.forName("org.springframework.amqp.rabbit.AsyncRabbitTemplate"));
    }

    private void assertDuplicateDoesNotInvoke(String key, IdempotencyState state) throws Exception {
        int before = probe.invocations("order.duplicates");
        idempotencyStore.setState(key, state);

        publisher.publish(
                "order.duplicates",
                new OrderCreated(key),
                PublishOptions.builder().idempotencyKey(key).partitionKey(key).build()
        ).block(WAIT);

        assertTrue(idempotencyStore.awaitTryStarts(key, 1, WAIT));
        assertRemains(() -> probe.invocations("order.duplicates") == before, Duration.ofMillis(300));

        idempotencyStore.setState(key, IdempotencyState.SUCCESS);
        assertTrue(idempotencyStore.awaitTryStarts(key, 2, WAIT));
        assertRemains(() -> probe.invocations("order.duplicates") == before, Duration.ofMillis(300));
    }

    private String status(String id) {
        return databaseClient.sql("select status from message_outbox where id = :id")
                .bind("id", id)
                .map((row, metadata) -> row.get("status", String.class))
                .one()
                .block(Duration.ofSeconds(5));
    }

    private void assertCounter(String metric, String eventName, String status, double expected) {
        assertEquals(expected, meterRegistry.get(metric)
                .tags("runtime", "webflux", "transport", "kafka", "event_name", eventName, "status", status)
                .counter().count());
    }

    private void awaitCounterAtLeast(String metric, String eventName, String status, double expected) throws Exception {
        await(() -> {
            io.micrometer.core.instrument.Counter counter = meterRegistry.find(metric)
                    .tags("runtime", "webflux", "transport", "kafka", "event_name", eventName, "status", status)
                    .counter();
            return counter != null && counter.count() >= expected;
        }, WAIT);
    }

    private static void await(BooleanSupplier condition, Duration timeout) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        assertTrue(condition.getAsBoolean(), "condition was not met within " + timeout);
    }

    private static void assertRemains(BooleanSupplier condition, Duration duration) throws Exception {
        long deadline = System.nanoTime() + duration.toNanos();
        while (System.nanoTime() < deadline) {
            assertTrue(condition.getAsBoolean());
            Thread.sleep(10);
        }
    }

    @SpringBootConfiguration
    @EnableAutoConfiguration
    static class SampleApplication {

        public static void main(String[] args) {
            SpringApplication.run(SampleApplication.class, args);
        }

        @Bean
        SampleProbe sampleProbe() {
            return new SampleProbe();
        }

        @Bean
        SampleListener sampleListener(SampleProbe probe) {
            return new SampleListener(probe);
        }

        @Bean
        SampleReactiveIdempotencyStore sampleReactiveIdempotencyStore() {
            return new SampleReactiveIdempotencyStore();
        }

        @Bean
        io.r2dbc.spi.ConnectionFactory r2dbcConnectionFactory() {
            return ConnectionFactories.get(
                    "r2dbc:h2:mem:///" + UUID.randomUUID() + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1"
            );
        }

        @Bean
        SampleController sampleController(ReactiveReliablePublisher publisher, SampleProbe probe) {
            return new SampleController(publisher, probe);
        }
    }

    @RestController
    static final class SampleController {
        private final ReactiveReliablePublisher publisher;
        private final SampleProbe probe;

        SampleController(ReactiveReliablePublisher publisher, SampleProbe probe) {
            this.publisher = publisher;
            this.probe = probe;
        }

        @PostMapping("/orders/{orderId}/events")
        Mono<org.springframework.http.ResponseEntity<Void>> publish(@PathVariable("orderId") String orderId) {
            return Mono.defer(() -> {
                        probe.endpointThread(Thread.currentThread().getName());
                        return publisher.publish(
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
                    })
                    .thenReturn(org.springframework.http.ResponseEntity.accepted().build());
        }
    }

    static final class SampleListener {
        private final SampleProbe probe;

        SampleListener(SampleProbe probe) {
            this.probe = probe;
        }

        @ReactiveReliableListener("order.published")
        Mono<Void> published(ReliableMessage<OrderCreated> message) {
            return Mono.fromRunnable(() -> probe.record("order.published", message));
        }

        @ReactiveReliableListener("order.idempotent")
        Mono<Void> idempotent(ReliableMessage<OrderCreated> message) {
            return Mono.fromRunnable(() -> probe.record("order.idempotent", message));
        }

        @ReactiveReliableListener("order.duplicates")
        Mono<Void> duplicates(ReliableMessage<OrderCreated> message) {
            return Mono.fromRunnable(() -> probe.record("order.duplicates", message));
        }

        @ReactiveReliableListener("order.retry")
        Mono<Void> retry(ReliableMessage<OrderCreated> message) {
            return Mono.fromRunnable(() -> {
                probe.record("order.retry", message);
                if (probe.invocations("order.retry") == 1) {
                    throw new IllegalStateException("transient failure");
                }
            });
        }

        @ReactiveReliableListener("order.outbox")
        Mono<Void> outbox(ReliableMessage<OrderCreated> message) {
            return Mono.fromRunnable(() -> probe.record("order.outbox", message));
        }
    }

    static final class SampleProbe {
        private final ConcurrentMap<String, AtomicInteger> invocations = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, ReliableMessage<OrderCreated>> messages = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, String> listenerThreads = new ConcurrentHashMap<>();
        private final AtomicReference<String> endpointThread = new AtomicReference<>();

        void record(String eventName, ReliableMessage<OrderCreated> message) {
            messages.put(eventName, message);
            listenerThreads.put(eventName, Thread.currentThread().getName());
            invocations.computeIfAbsent(eventName, ignored -> new AtomicInteger()).incrementAndGet();
        }

        int invocations(String eventName) {
            AtomicInteger count = invocations.get(eventName);
            return count == null ? 0 : count.get();
        }

        ReliableMessage<OrderCreated> lastMessage(String eventName) {
            return messages.get(eventName);
        }

        String listenerThread(String eventName) {
            return listenerThreads.get(eventName);
        }

        void endpointThread(String thread) {
            endpointThread.set(thread);
        }

        String endpointThread() {
            return endpointThread.get();
        }

        boolean awaitInvocations(String eventName, int expected, Duration timeout) throws InterruptedException {
            long deadline = System.nanoTime() + timeout.toNanos();
            while (invocations(eventName) < expected && System.nanoTime() < deadline) {
                Thread.sleep(10);
            }
            return invocations(eventName) >= expected;
        }
    }

    static final class SampleReactiveIdempotencyStore implements ReactiveIdempotencyStore {
        private final ConcurrentMap<String, IdempotencyState> states = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, AtomicInteger> tryStarts = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, Boolean> retryableFailures = new ConcurrentHashMap<>();

        @Override
        public Mono<IdempotencyStartResult> tryStart(String key, Duration ttl) {
            return Mono.fromSupplier(() -> {
                tryStarts.computeIfAbsent(key, ignored -> new AtomicInteger()).incrementAndGet();
                IdempotencyState state = states.putIfAbsent(key, IdempotencyState.PROCESSING);
                if (state == IdempotencyState.FAILED && retryableFailures.remove(key) != null) {
                    states.put(key, IdempotencyState.PROCESSING);
                    return IdempotencyStartResult.startAccepted();
                }
                return state == null
                        ? IdempotencyStartResult.startAccepted()
                        : IdempotencyStartResult.duplicate(state);
            });
        }

        @Override
        public Mono<Void> markSuccess(String key) {
            return Mono.fromRunnable(() -> states.put(key, IdempotencyState.SUCCESS));
        }

        @Override
        public Mono<Void> markFailed(String key, Throwable error) {
            return Mono.fromRunnable(() -> {
                states.put(key, IdempotencyState.FAILED);
                retryableFailures.put(key, true);
            });
        }

        void setState(String key, IdempotencyState state) {
            states.put(key, state);
            retryableFailures.remove(key);
        }

        boolean awaitState(String key, IdempotencyState expected, Duration timeout) throws InterruptedException {
            long deadline = System.nanoTime() + timeout.toNanos();
            while (states.get(key) != expected && System.nanoTime() < deadline) {
                Thread.sleep(10);
            }
            return states.get(key) == expected;
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

    record OrderCreated(String orderId) {
    }
}
