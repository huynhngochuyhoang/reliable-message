package io.github.huynhngochuyhoang.reliablemessage.outbox.r2dbc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.huynhngochuyhoang.reliablemessage.core.MessageStatus;
import io.github.huynhngochuyhoang.reliablemessage.core.PublishOptions;
import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessageHeaders;
import io.github.huynhngochuyhoang.reliablemessage.webflux.OutboxMessage;
import io.github.huynhngochuyhoang.reliablemessage.webflux.ReactiveReliablePublisher;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.r2dbc.core.DatabaseClient;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.*;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Testcontainers(disabledWithoutDocker = true)
class R2dbcOutboxPostgresIntegrationTest {

    private static final Instant NOW = Instant.parse("2026-05-18T00:00:00Z");
    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    private DatabaseClient databaseClient;
    private R2dbcOutboxStore store;
    @BeforeEach
    void setUp() {
        databaseClient = DatabaseClient.create(connectionFactory());
        store = store(new R2dbcOutboxProperties());
        StepVerifier.create(databaseClient.sql("drop table if exists message_outbox").fetch().rowsUpdated().then())
                .verifyComplete();
    }
    @Test
    void savePersistsPayloadHeadersMetadataCreatedAtAndPendingStatus() {
        OutboxMessage message = message("event-save", "order.created", NOW, PublishOptions.builder()
                .aggregateId("order-1")
                .idempotencyKey("idem-save")
                .correlationId("corr-save")
                .partitionKey("partition-save")
                .header("trace-id", "trace-save")
                .build());

        StepVerifier.create(store.initializeSchema()
                        .then(store.save(message))
                        .then(databaseClient.sql("""
                                select event_name, aggregate_id, idempotency_key, partition_key, payload::text as payload,
                                       headers::text as headers, status, retry_count, created_at, published_at
                                from message_outbox
                                where id = :id
                                """)
                                .bind("id", "event-save")
                                .map((row, metadata) -> new StoredRow(
                                        row.get("event_name", String.class),
                                        row.get("aggregate_id", String.class),
                                        row.get("idempotency_key", String.class),
                                        row.get("partition_key", String.class),
                                        row.get("payload", String.class),
                                        row.get("headers", String.class),
                                        row.get("status", String.class),
                                        row.get("retry_count", Integer.class),
                                        row.get("created_at", LocalDateTime.class),
                                        row.get("published_at", LocalDateTime.class)
                                ))
                                .one()))
                .assertNext(row -> {
                    assertThat(row.eventName()).isEqualTo("order.created");
                    assertThat(row.aggregateId()).isEqualTo("order-1");
                    assertThat(row.idempotencyKey()).isEqualTo("idem-save");
                    assertThat(row.partitionKey()).isEqualTo("partition-save");
                    assertThat(row.payload()).contains("\"orderId\"").contains("\"order-1\"");
                    assertThat(row.headers()).contains("\"trace-id\"").contains("\"trace-save\"");
                    assertThat(row.headers()).contains("\"x-correlation-id\"").contains("\"corr-save\"");
                    assertThat(row.status()).isEqualTo(MessageStatus.PENDING.name());
                    assertThat(row.retryCount()).isEqualTo(0);
                    assertThat(row.createdAt()).isEqualTo(LocalDateTime.ofInstant(NOW, ZoneOffset.UTC));
                    assertThat(row.publishedAt()).isNull();
                })
                .verifyComplete();
    }
    @Test
    void postgresqlJsonSchemaSavesReadsAndClaimsJsonbRows() {
        R2dbcOutboxProperties properties = new R2dbcOutboxProperties();
        properties.getSchema().setPayloadStorage(R2dbcOutboxProperties.PayloadStorage.JSON);
        R2dbcOutboxStore jsonStore = store(properties);

        StepVerifier.create(jsonStore.initializeSchema()
                        .then(jsonStore.save(message("event-json", "order.created", NOW, PublishOptions.builder()
                                .correlationId("corr-json")
                                .header("trace-id", "trace-json")
                                .build())))
                        .thenMany(jsonStore.findPending(10))
                        .single())
                .assertNext(message -> {
                    assertThat(message.status()).isEqualTo(MessageStatus.PROCESSING);
                    assertThat(((JsonNode) message.payload()).get("orderId").asText()).isEqualTo("order-1");
                    assertThat(message.headers()).containsEntry("trace-id", "trace-json");
                    assertThat(message.toPublishOptions().correlationId()).isEqualTo("corr-json");
                })
                .verifyComplete();
    }
    @Test
    void postgresqlClaimStrategyClaimsEligibleRowsInCreatedAtOrder() {
        StepVerifier.create(store.initializeSchema()
                        .then(store.save(message("pending-2", "order.second", NOW.plusSeconds(2), PublishOptions.empty())))
                        .then(store.save(message("pending-1", "order.first", NOW.plusSeconds(1), PublishOptions.empty())))
                        .then(store.save(message("published", "order.published", NOW.plusSeconds(3), PublishOptions.empty())))
                        .then(markStatus("published", MessageStatus.PUBLISHED))
                        .thenMany(store.findPending(10).map(OutboxMessage::id).collectList()))
                .assertNext(ids -> assertThat(ids).containsExactly("pending-1", "pending-2"))
                .verifyComplete();
    }
    @Test
    void concurrentPostgresqlClaimersDoNotReceiveSameRows() {
        StepVerifier.create(store.initializeSchema()
                        .thenMany(Flux.range(1, 12)
                                .concatMap(index -> store.save(message("event-" + index, "order.created", NOW.plusSeconds(index), PublishOptions.empty()))))
                        .thenMany(Flux.merge(store.findPending(8), store.findPending(8)).map(OutboxMessage::id).collectList()))
                .assertNext(ids -> assertThat(ids).doesNotHaveDuplicates().hasSize(12))
                .verifyComplete();
    }
    @Test
    void concurrentFlushersPublishEachClaimedRowOnceAndMarkPublished() {
        RecordingPublisher publisher = new RecordingPublisher();
        R2dbcOutboxProperties properties = flusherProperties();
        ReactiveOutboxFlushScheduler first = new ReactiveOutboxFlushScheduler(store, publisher, properties, Clock.fixed(NOW, ZoneOffset.UTC));
        ReactiveOutboxFlushScheduler second = new ReactiveOutboxFlushScheduler(store, publisher, properties, Clock.fixed(NOW, ZoneOffset.UTC));

        StepVerifier.create(store.initializeSchema()
                        .thenMany(Flux.range(1, 12)
                                .concatMap(index -> store.save(message("event-" + index, "order.created", NOW.plusSeconds(index),
                                        PublishOptions.builder().idempotencyKey("event-" + index).build()))))
                        .thenMany(Flux.merge(first.flushBatch(), second.flushBatch()).collectList()))
                .assertNext(counts -> assertThat(counts.stream().mapToInt(Integer::intValue).sum()).isEqualTo(12))
                .verifyComplete();

        assertThat(publisher.idempotencyKeys()).doesNotHaveDuplicates().hasSize(12);
        StepVerifier.create(countByStatus(MessageStatus.PUBLISHED))
                .expectNext(12L)
                .verifyComplete();
    }
    @Test
    void publishFailureMarksFailedAndRowRemainsRetryable() {
        RecordingPublisher publisher = new RecordingPublisher();
        publisher.fail(new IllegalStateException("broker unavailable"));
        R2dbcOutboxProperties properties = flusherProperties();
        properties.setRetryDelay(Duration.ofSeconds(30));
        ReactiveOutboxFlushScheduler scheduler = new ReactiveOutboxFlushScheduler(
                store,
                publisher,
                properties,
                Clock.fixed(NOW, ZoneOffset.UTC)
        );

        StepVerifier.create(store.initializeSchema()
                        .then(store.save(message("event-failed", "order.created", NOW, PublishOptions.empty())))
                        .then(scheduler.flushBatch()))
                .expectNext(0)
                .verifyComplete();

        StepVerifier.create(statusAndRetry("event-failed"))
                .assertNext(row -> {
                    assertThat(row.status()).isEqualTo(MessageStatus.FAILED.name());
                    assertThat(row.retryCount()).isEqualTo(1);
                    assertThat(row.lastError()).isEqualTo("broker unavailable");
                })
                .verifyComplete();

        StepVerifier.create(setNextRetryAt("event-failed", NOW.minusSeconds(1))
                        .thenMany(store.findPending(1))
                        .single())
                .expectNextMatches(message -> message.retryCount() == 1 && "event-failed".equals(message.id()))
                .verifyComplete();
    }
    @Test
    void binaryPayloadStorageFailsBeforeSchemaInitialization() {
        R2dbcOutboxProperties properties = new R2dbcOutboxProperties();
        properties.getSchema().setPayloadStorage(R2dbcOutboxProperties.PayloadStorage.BINARY);

        assertThatThrownBy(() -> store(properties))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("binary payload storage requires runtime codec/storage support that is not implemented yet");
    }

    private R2dbcOutboxStore store(R2dbcOutboxProperties properties) {
        OutboxSchema schema = new OutboxSchemaResolver(properties).resolve(OutboxDatabaseDialect.POSTGRESQL);
        return new R2dbcOutboxStore(databaseClient, new ObjectMapper(), Clock.fixed(NOW, ZoneOffset.UTC), schema);
    }

    private Mono<Void> markStatus(String id, MessageStatus status) {
        return databaseClient.sql("update message_outbox set status = :status where id = :id")
                .bind("status", status.name())
                .bind("id", id)
                .fetch()
                .rowsUpdated()
                .then();
    }

    private Mono<Void> setNextRetryAt(String id, Instant nextRetryAt) {
        return databaseClient.sql("update message_outbox set next_retry_at = :nextRetryAt where id = :id")
                .bind("nextRetryAt", LocalDateTime.ofInstant(nextRetryAt, ZoneOffset.UTC))
                .bind("id", id)
                .fetch()
                .rowsUpdated()
                .then();
    }

    private Mono<Long> countByStatus(MessageStatus status) {
        return databaseClient.sql("select count(*) as count from message_outbox where status = :status")
                .bind("status", status.name())
                .map((row, metadata) -> row.get("count", Long.class))
                .one();
    }

    private Mono<RowState> statusAndRetry(String id) {
        return databaseClient.sql("select status, retry_count, last_error from message_outbox where id = :id")
                .bind("id", id)
                .map((row, metadata) -> new RowState(
                        row.get("status", String.class),
                        row.get("retry_count", Integer.class),
                        row.get("last_error", String.class)
                ))
                .one();
    }

    private static R2dbcOutboxProperties flusherProperties() {
        R2dbcOutboxProperties properties = new R2dbcOutboxProperties();
        properties.setEnabled(true);
        properties.setFlushEnabled(true);
        properties.setBatchSize(6);
        properties.setRetryDelay(Duration.ofSeconds(30));
        properties.setPublishTimeout(Duration.ofSeconds(5));
        return properties;
    }

    private static ConnectionFactory connectionFactory() {
        return io.r2dbc.spi.ConnectionFactories.get(ConnectionFactoryOptions.builder()
                .option(ConnectionFactoryOptions.DRIVER, "postgresql")
                .option(ConnectionFactoryOptions.HOST, postgres.getHost())
                .option(ConnectionFactoryOptions.PORT, postgres.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT))
                .option(ConnectionFactoryOptions.DATABASE, postgres.getDatabaseName())
                .option(ConnectionFactoryOptions.USER, postgres.getUsername())
                .option(ConnectionFactoryOptions.PASSWORD, postgres.getPassword())
                .build());
    }

    private static OutboxMessage message(String id, String eventName, Instant createdAt, PublishOptions options) {
        PublishOptions safeOptions = options == null ? PublishOptions.empty() : options;
        Map<String, String> headers = new LinkedHashMap<>(safeOptions.headers());
        if (safeOptions.correlationId() != null && !safeOptions.correlationId().isBlank()) {
            headers.put(ReliableMessageHeaders.CORRELATION_ID, safeOptions.correlationId());
        }
        return new OutboxMessage(
                id,
                eventName,
                safeOptions.aggregateId(),
                safeOptions.idempotencyKey(),
                safeOptions.partitionKey(),
                new OrderCreated("order-1"),
                headers,
                MessageStatus.PENDING,
                0,
                null,
                createdAt,
                null,
                null
        );
    }

    private record OrderCreated(String orderId) {
    }

    private record StoredRow(
            String eventName,
            String aggregateId,
            String idempotencyKey,
            String partitionKey,
            String payload,
            String headers,
            String status,
            int retryCount,
            LocalDateTime createdAt,
            LocalDateTime publishedAt
    ) {
    }

    private record RowState(String status, int retryCount, String lastError) {
    }

    private static final class RecordingPublisher implements ReactiveReliablePublisher {
        private final List<String> idempotencyKeys = new CopyOnWriteArrayList<>();
        private RuntimeException failure;
        @Override
        public Mono<Void> publish(String eventName, Object payload, PublishOptions options) {
            return Mono.defer(() -> {
                idempotencyKeys.add(options.idempotencyKey());
                if (failure != null) {
                    return Mono.error(failure);
                }
                return Mono.empty();
            });
        }

        void fail(RuntimeException failure) {
            this.failure = failure;
        }

        List<String> idempotencyKeys() {
            return new ArrayList<>(idempotencyKeys);
        }
    }
}
