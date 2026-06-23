package io.github.huynhngochuyhoang.reliablemessage.kafka.webflux;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.huynhngochuyhoang.reliablemessage.core.MessageStatus;
import io.github.huynhngochuyhoang.reliablemessage.core.PublishOptions;
import io.github.huynhngochuyhoang.reliablemessage.observability.MessageObservability;
import io.github.huynhngochuyhoang.reliablemessage.outbox.r2dbc.*;
import io.github.huynhngochuyhoang.reliablemessage.webflux.OutboxMessage;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.observation.ObservationRegistry;
import io.r2dbc.spi.ConnectionFactories;
import org.junit.jupiter.api.Test;
import org.springframework.r2dbc.core.DatabaseClient;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.KafkaSender;
import reactor.test.StepVerifier;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

class KafkaWebFluxR2dbcOutboxFailureIntegrationTest {

    private static final Instant NOW = Instant.parse("2026-06-23T00:00:00Z");

    @Test
    void kafkaPublishFailureMarksR2dbcOutboxFailedAndNeverPublished() {
        DatabaseClient databaseClient = DatabaseClient.create(ConnectionFactories.get(
                "r2dbc:h2:mem:///" + UUID.randomUUID() + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1"
        ));
        R2dbcOutboxProperties outboxProperties = new R2dbcOutboxProperties();
        outboxProperties.setBatchSize(1);
        outboxProperties.setRetryDelay(Duration.ofSeconds(10));
        outboxProperties.setPublishTimeout(Duration.ofSeconds(2));
        outboxProperties.getSchema().setPayloadBytesColumnType("bytea");
        OutboxSchema schema = new OutboxSchemaResolver(outboxProperties).resolve(OutboxDatabaseDialect.GENERIC);
        Clock clock = Clock.fixed(NOW, ZoneOffset.UTC);
        R2dbcOutboxStore store = new R2dbcOutboxStore(databaseClient, new ObjectMapper(), clock, schema);

        @SuppressWarnings("unchecked")
        KafkaSender<String, byte[]> sender = org.mockito.Mockito.mock(KafkaSender.class);
        when(sender.send(any(org.reactivestreams.Publisher.class)))
                .thenReturn(Flux.error(new IllegalStateException("broker unavailable")));
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        ReactiveKafkaReliableMessageProperties kafkaProperties = new ReactiveKafkaReliableMessageProperties();
        ReactiveKafkaReliablePublisher publisher = new ReactiveKafkaReliablePublisher(
                sender,
                new JacksonReliableMessageSerializer(new ObjectMapper()),
                kafkaProperties,
                clock,
                new MessageObservability(registry, ObservationRegistry.NOOP)
        );
        ReactiveOutboxFlushScheduler flusher = new ReactiveOutboxFlushScheduler(
                store, publisher, outboxProperties, clock
        );
        OutboxMessage message = OutboxMessage.pending(
                "order.outbox.failed",
                new OrderCreated("order-1"),
                PublishOptions.builder().partitionKey("order-1").build(),
                NOW
        );

        StepVerifier.create(store.initializeSchema()
                        .then(store.save(message))
                        .then(flusher.flushBatch()))
                .expectNext(0)
                .verifyComplete();

        RowState state = databaseClient.sql("""
                        select status, retry_count, next_retry_at, published_at, last_error
                        from message_outbox
                        where id = :id
                        """)
                .bind("id", message.id())
                .map((row, metadata) -> new RowState(
                        row.get("status", String.class),
                        row.get("retry_count", Integer.class),
                        row.get("next_retry_at", java.time.LocalDateTime.class),
                        row.get("published_at", java.time.LocalDateTime.class),
                        row.get("last_error", String.class)
                ))
                .one()
                .block(Duration.ofSeconds(2));

        assertEquals(MessageStatus.FAILED.name(), state.status());
        assertEquals(1, state.retryCount());
        assertEquals(java.time.LocalDateTime.ofInstant(NOW.plusSeconds(10), ZoneOffset.UTC), state.nextRetryAt());
        assertNull(state.publishedAt());
        assertEquals("broker unavailable", state.lastError());
        assertEquals(1.0, registry.get("message_publish_failed_total")
                .tags("runtime", "webflux", "transport", "kafka",
                        "event_name", "order.outbox.failed", "status", "failed")
                .counter().count());
    }

    record OrderCreated(String orderId) {
    }

    private record RowState(
            String status,
            Integer retryCount,
            java.time.LocalDateTime nextRetryAt,
            java.time.LocalDateTime publishedAt,
            String lastError
    ) {
    }
}
