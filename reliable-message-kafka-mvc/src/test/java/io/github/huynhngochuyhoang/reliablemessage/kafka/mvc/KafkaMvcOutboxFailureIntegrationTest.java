package io.github.huynhngochuyhoang.reliablemessage.kafka.mvc;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.huynhngochuyhoang.reliablemessage.core.MessageStatus;
import io.github.huynhngochuyhoang.reliablemessage.core.PublishOptions;
import io.github.huynhngochuyhoang.reliablemessage.observability.MessageObservability;
import io.github.huynhngochuyhoang.reliablemessage.outbox.jdbc.JdbcOutboxProperties;
import io.github.huynhngochuyhoang.reliablemessage.outbox.jdbc.JdbcOutboxPublisher;
import io.github.huynhngochuyhoang.reliablemessage.outbox.jdbc.JdbcOutboxStore;
import io.github.huynhngochuyhoang.reliablemessage.outbox.jdbc.OutboxFlushScheduler;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.observation.ObservationRegistry;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class KafkaMvcOutboxFailureIntegrationTest {

    @Test
    void kafkaPublishFailureMarksOutboxRowFailedWithoutMarkingPublished() {
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setURL("jdbc:h2:mem:s8-kafka-outbox-failure;DB_CLOSE_DELAY=-1");
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        JdbcOutboxStore store = new JdbcOutboxStore(jdbcTemplate, new ObjectMapper());
        store.initializeSchema();

        Clock clock = Clock.systemUTC();
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        MessageObservability observability = new MessageObservability(registry, ObservationRegistry.NOOP);
        JdbcOutboxPublisher outboxPublisher = new JdbcOutboxPublisher(store, clock, observability);
        outboxPublisher.publishLater(
                "order.failed",
                new OrderCreated("order-failed"),
                PublishOptions.builder().partitionKey("order-failed").build()
        );

        KafkaTemplate<String, byte[]> kafkaTemplate = failedKafkaTemplate();
        KafkaReliablePublisher publisher = new KafkaReliablePublisher(
                kafkaTemplate,
                new JacksonReliableMessageSerializer(new ObjectMapper()),
                new KafkaReliableMessageProperties(),
                clock,
                observability
        );
        JdbcOutboxProperties properties = new JdbcOutboxProperties();
        properties.setRetryDelay(Duration.ofSeconds(1));
        OutboxFlushScheduler scheduler = new OutboxFlushScheduler(store, publisher, properties, clock, observability);

        assertEquals(0, scheduler.flushBatch());

        Map<String, Object> failed = jdbcTemplate.queryForMap(
                "select status, retry_count, next_retry_at, published_at, last_error from message_outbox"
        );
        assertEquals(MessageStatus.FAILED.name(), failed.get("STATUS"));
        assertEquals(1, failed.get("RETRY_COUNT"));
        assertNotNull(failed.get("NEXT_RETRY_AT"));
        assertNull(failed.get("PUBLISHED_AT"));
        assertEquals("Failed to publish Kafka reliable message", failed.get("LAST_ERROR"));
        assertNull(registry.find("message_publish_total")
                .tags("runtime", "mvc", "transport", "kafka", "event_name", "order.failed", "status", "success")
                .counter());
        assertEquals(1.0, registry.find("message_publish_failed_total")
                .tags("runtime", "mvc", "transport", "kafka", "event_name", "order.failed", "status", "failed")
                .counter().count());
    }

    @SuppressWarnings("unchecked")
    private static KafkaTemplate<String, byte[]> failedKafkaTemplate() {
        KafkaTemplate<String, byte[]> kafkaTemplate = mock(KafkaTemplate.class);
        CompletableFuture<?> failed = CompletableFuture.failedFuture(new IllegalStateException("broker unavailable"));
        when(kafkaTemplate.send(any(org.apache.kafka.clients.producer.ProducerRecord.class)))
                .thenReturn((CompletableFuture) failed);
        return kafkaTemplate;
    }

    record OrderCreated(String orderId) {
    }
}
