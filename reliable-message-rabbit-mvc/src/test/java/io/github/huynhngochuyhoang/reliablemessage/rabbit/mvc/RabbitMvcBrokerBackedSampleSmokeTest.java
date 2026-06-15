package io.github.huynhngochuyhoang.reliablemessage.rabbit.mvc;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.huynhngochuyhoang.reliablemessage.core.PublishOptions;
import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessage;
import io.github.huynhngochuyhoang.reliablemessage.core.serialization.MessageSerializer;
import io.github.huynhngochuyhoang.reliablemessage.mvc.*;
import io.github.huynhngochuyhoang.reliablemessage.observability.MessageObservability;
import io.github.huynhngochuyhoang.reliablemessage.outbox.jdbc.JdbcOutboxProperties;
import io.github.huynhngochuyhoang.reliablemessage.outbox.jdbc.JdbcOutboxPublisher;
import io.github.huynhngochuyhoang.reliablemessage.outbox.jdbc.JdbcOutboxStore;
import io.github.huynhngochuyhoang.reliablemessage.outbox.jdbc.OutboxFlushScheduler;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.observation.ObservationRegistry;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.AsyncRabbitTemplate;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
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
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;
import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(
        classes = RabbitMvcBrokerBackedSampleSmokeTest.SampleApplication.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "message.reliability.transport=rabbit",
                "message.reliability.service-name=orders",
                "message.reliability.rabbit.exchange=s2.mvc.rabbit.events",
                "message.reliability.rabbit.publisher-confirm=false",
                "message.reliability.retry.attempts=2",
                "message.reliability.retry.backoff[0]=100ms",
                "message.reliability.outbox.flush-delay=60s",
                "spring.datasource.url=jdbc:h2:mem:s2-mvc-rabbit;MODE=PostgreSQL;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=false",
                "spring.datasource.username=sa",
                "spring.datasource.password="
        }
)
class RabbitMvcBrokerBackedSampleSmokeTest {

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
    TestRestTemplate restTemplate;

    @Autowired
    ReliablePublisher reliablePublisher;

    @Autowired
    RabbitTemplate rabbitTemplate;

    @Autowired
    OutboxPublisher outboxPublisher;

    @Autowired
    OutboxStore outboxStore;

    @Autowired
    OutboxFlushScheduler outboxFlushScheduler;

    @Autowired
    SampleInvocationProbe probe;

    @Autowired
    SampleIdempotencyStore idempotencyStore;

    @Autowired
    ConfigurableApplicationContext context;

    @Test
    void httpEndpointPublishesRabbitEventAndListenerProcessesItOnce() throws Exception {
        ResponseEntity<Void> response = restTemplate.postForEntity(
                "http://localhost:" + port + "/orders/order-1/events",
                null,
                Void.class
        );

        assertEquals(HttpStatus.ACCEPTED, response.getStatusCode());
        assertTrue(probe.orders.await(Duration.ofSeconds(10)));
        assertEquals(1, probe.orders.invocations());
        assertEquals("order-1", probe.orders.lastOrderId());
        assertNotNull(probe.orders.lastMessage());
        assertEquals("order-1", probe.orders.lastMessage().aggregateId());
        assertEquals("order-1-event", probe.orders.lastMessage().idempotencyKey());
        assertEquals("order-1-correlation", probe.orders.lastMessage().correlationId());
    }

    @Test
    void duplicateSuccessIsAckedAndSkipsHandler() throws Exception {
        idempotencyStore.duplicates.put("duplicate-success-key", IdempotencyState.SUCCESS);

        reliablePublisher.publish(
                "order.duplicate",
                new OrderCreated("duplicate-success"),
                PublishOptions.builder().idempotencyKey("duplicate-success-key").build()
        );

        assertTrue(idempotencyStore.awaitTryStart("duplicate-success-key", Duration.ofSeconds(10)));
        assertEquals(1, idempotencyStore.tryStartCount("duplicate-success-key"));
        assertFalse(probe.duplicates.await(Duration.ofMillis(250)));
        assertEquals(0, probe.duplicates.invocations());
    }

    @Test
    void duplicateProcessingIsNotAckedAsSuccessAndSkipsHandler() throws Exception {
        idempotencyStore.duplicates.put("duplicate-processing-key", IdempotencyState.PROCESSING);

        reliablePublisher.publish(
                "order.duplicate",
                new OrderCreated("duplicate-processing"),
                PublishOptions.builder().idempotencyKey("duplicate-processing-key").build()
        );

        assertTrue(idempotencyStore.awaitTryStart("duplicate-processing-key", Duration.ofSeconds(10)));
        idempotencyStore.duplicates.put("duplicate-processing-key", IdempotencyState.SUCCESS);
        assertTrue(idempotencyStore.awaitTryStartCount("duplicate-processing-key", 2, Duration.ofSeconds(10)));
        assertEquals(0, probe.duplicates.invocations());
    }

    @Test
    void duplicateFailedIsNotAckedAsSuccessAndSkipsHandler() throws Exception {
        idempotencyStore.duplicates.put("duplicate-failed-key", IdempotencyState.FAILED);

        reliablePublisher.publish(
                "order.duplicate",
                new OrderCreated("duplicate-failed"),
                PublishOptions.builder().idempotencyKey("duplicate-failed-key").build()
        );

        assertTrue(idempotencyStore.awaitTryStart("duplicate-failed-key", Duration.ofSeconds(10)));
        idempotencyStore.duplicates.put("duplicate-failed-key", IdempotencyState.SUCCESS);
        assertTrue(idempotencyStore.awaitTryStartCount("duplicate-failed-key", 2, Duration.ofSeconds(10)));
        assertEquals(0, probe.duplicates.invocations());
    }

    @Test
    void listenerFailureIsRetriedByRabbitAndThenSucceeds() throws Exception {
        reliablePublisher.publish("order.retry", new OrderCreated("retry-1"), PublishOptions.empty());

        assertTrue(probe.retry.await(Duration.ofSeconds(15)));
        assertEquals(2, probe.retry.invocations());
    }

    @Test
    void terminalListenerFailureIsRoutedToDlq() {
        reliablePublisher.publish("order.dlq", new OrderCreated("dlq-1"), PublishOptions.empty());

        Message dlqMessage = rabbitTemplate.receive("orders.order.dlq.dlq", 15_000);
        assertNotNull(dlqMessage);
        assertEquals(2, dlqMessage.getMessageProperties().getHeaders().get("x-retry-count"));
    }

    @Test
    void jdbcOutboxFlushPublishesThroughRabbitAndMarksPublishedAfterSuccess() throws Exception {
        outboxPublisher.publishLater(
                "order.outbox",
                new OrderCreated("outbox-1"),
                PublishOptions.builder()
                        .aggregateId("outbox-1")
                        .idempotencyKey("outbox-1-event")
                        .correlationId("outbox-1-correlation")
                        .build()
        );

        assertEquals(1, outboxStore.findForAdmin(10).size());
        assertEquals(1, outboxFlushScheduler.flushBatch());

        assertTrue(probe.outbox.await(Duration.ofSeconds(10)));
        assertEquals(1, probe.outbox.invocations());
        assertEquals("outbox-1", probe.outbox.lastOrderId());
        assertEquals(0, outboxStore.findPending(10).size());
        assertEquals(0, outboxFlushScheduler.flushBatch());
    }

    @Test
    void sampleUsesMvcRabbitEventBeansOnly() {
        assertTrue(context.containsBean("reliablePublisher"));
        assertTrue(context.containsBean("rabbitReliableListenerRegistrar"));
        assertTrue(context.containsBean("jdbcOutboxPublisher"));
        assertTrue(context.containsBean("outboxFlushScheduler"));
        assertFalse(context.containsBean("reactiveRabbitBridgePublisher"));
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
        @Bean
        Clock reliableMessageClock() {
            return Clock.systemUTC();
        }

        @Bean
        DataSource dataSource() {
            JdbcDataSource dataSource = new JdbcDataSource();
            dataSource.setURL("jdbc:h2:mem:s2-mvc-rabbit;MODE=PostgreSQL;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=false");
            dataSource.setUser("sa");
            dataSource.setPassword("");
            return dataSource;
        }


        @Bean
        MessageSerializer reliableMessageSerializer(ObjectMapper objectMapper) {
            return new JacksonReliableMessageSerializer(objectMapper);
        }

        @Bean
        MessageObservability messageObservability() {
            return new MessageObservability(new SimpleMeterRegistry(), ObservationRegistry.NOOP);
        }

        @Bean
        RabbitAdmin rabbitAdmin(ConnectionFactory connectionFactory) {
            return new RabbitAdmin(connectionFactory);
        }

        @Bean
        RabbitTopologyAutoConfigurer rabbitTopologyAutoConfigurer(
                RabbitAdmin rabbitAdmin,
                RabbitReliableMessageProperties properties
        ) {
            return new RabbitTopologyAutoConfigurer(rabbitAdmin, properties);
        }

        @Bean
        RabbitRetryStrategy rabbitRetryStrategy(
                RabbitTemplate rabbitTemplate,
                RabbitReliableMessageProperties properties,
                MessageObservability observability
        ) {
            return new RabbitRetryStrategy(rabbitTemplate, properties, observability);
        }

        @Bean
        ReliablePublisher reliablePublisher(
                RabbitTemplate rabbitTemplate,
                MessageSerializer serializer,
                RabbitReliableMessageProperties properties,
                Clock clock,
                MessageObservability observability
        ) {
            return new RabbitReliablePublisher(rabbitTemplate, serializer, properties, clock, observability);
        }

        @Bean
        RabbitReliableListenerRegistrar rabbitReliableListenerRegistrar(
                ConnectionFactory connectionFactory,
                MessageSerializer serializer,
                RabbitReliableMessageProperties properties,
                MessageObservability observability,
                RabbitTopologyAutoConfigurer topologyAutoConfigurer,
                RabbitRetryStrategy retryStrategy,
                SampleIdempotencyStore idempotencyStore
        ) {
            return new RabbitReliableListenerRegistrar(
                    connectionFactory,
                    serializer,
                    properties,
                    observability,
                    topologyAutoConfigurer,
                    retryStrategy,
                    idempotencyStore
            );
        }

        @Bean
        JdbcTemplate jdbcTemplate(DataSource dataSource) {
            return new JdbcTemplate(dataSource);
        }


        @Bean
        OutboxStore outboxStore(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper, Clock clock) {
            JdbcOutboxStore store = new JdbcOutboxStore(jdbcTemplate, objectMapper, clock);
            store.initializeSchema();
            return store;
        }

        @Bean
        OutboxPublisher jdbcOutboxPublisher(
                OutboxStore outboxStore,
                Clock clock,
                MessageObservability observability
        ) {
            return new JdbcOutboxPublisher(outboxStore, clock, observability);
        }

        @Bean
        OutboxFlushScheduler outboxFlushScheduler(
                OutboxStore outboxStore,
                ReliablePublisher reliablePublisher,
                JdbcOutboxProperties properties,
                Clock clock,
                MessageObservability observability
        ) {
            return new OutboxFlushScheduler(outboxStore, reliablePublisher, properties, clock, observability);
        }

    }

    @RestController
    static class SampleOrderController {
        private final ReliablePublisher publisher;

        SampleOrderController(ReliablePublisher publisher) {
            this.publisher = publisher;
        }

        @PostMapping("/orders/{orderId}/events")
        ResponseEntity<Void> publishOrderCreated(@PathVariable("orderId") String orderId) {
            publisher.publish(
                    "order.created",
                    new OrderCreated(orderId),
                    PublishOptions.builder()
                            .aggregateId(orderId)
                            .idempotencyKey(orderId + "-event")
                            .correlationId(orderId + "-correlation")
                            .build()
            );
            return ResponseEntity.accepted().build();
        }
    }

    static class SampleOrderListener {
        private final SampleInvocationProbe probe;

        SampleOrderListener(SampleInvocationProbe probe) {
            this.probe = probe;
        }

        @ReliableListener("order.created")
        void onOrderCreated(ReliableMessage<OrderCreated> message) {
            probe.orders.record(message);
        }

        @ReliableListener("order.duplicate")
        void onDuplicate(ReliableMessage<OrderCreated> message) {
            probe.duplicates.record(message);
        }

        @ReliableListener("order.retry")
        void onRetry(ReliableMessage<OrderCreated> message) {
            probe.retry.record(message);
            if (probe.retry.invocations() == 1) {
                throw new IllegalStateException("retry once");
            }
        }

        @ReliableListener("order.dlq")
        void onDlq(ReliableMessage<OrderCreated> message) {
            throw new IllegalStateException("always fail");
        }

        @ReliableListener("order.outbox")
        void onOutbox(ReliableMessage<OrderCreated> message) {
            probe.outbox.record(message);
        }
    }

    static final class SampleInvocationProbe {
        final EventProbe orders = new EventProbe();
        final EventProbe duplicates = new EventProbe();
        final EventProbe retry = new EventProbe(2);
        final EventProbe outbox = new EventProbe();
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

    static final class SampleIdempotencyStore implements IdempotencyStore {
        private final ConcurrentMap<String, IdempotencyState> duplicates = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, AtomicInteger> tryStarts = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, CountDownLatch> firstTryStartLatches = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, CountDownLatch> secondTryStartLatches = new ConcurrentHashMap<>();

        @Override
        public IdempotencyStartResult tryStart(String key, Duration ttl) {
            int count = tryStarts.computeIfAbsent(key, ignored -> new AtomicInteger()).incrementAndGet();
            firstTryStartLatches.computeIfAbsent(key, ignored -> new CountDownLatch(1)).countDown();
            if (count >= 2) {
                secondTryStartLatches.computeIfAbsent(key, ignored -> new CountDownLatch(1)).countDown();
            }
            IdempotencyState duplicate = duplicates.get(key);
            return duplicate == null ? IdempotencyStartResult.startAccepted() : IdempotencyStartResult.duplicate(duplicate);
        }

        @Override
        public void markSuccess(String key) {
            duplicates.put(key, IdempotencyState.SUCCESS);
        }

        @Override
        public void markFailed(String key, Throwable error) {
            duplicates.put(key, IdempotencyState.FAILED);
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

    record OrderCreated(String orderId) {
    }
}
