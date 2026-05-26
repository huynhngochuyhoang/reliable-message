package io.github.huynhngochuyhoang.reliablemessage.outbox.r2dbc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.huynhngochuyhoang.reliablemessage.core.MessageStatus;
import io.github.huynhngochuyhoang.reliablemessage.webflux.OutboxMessage;
import io.github.huynhngochuyhoang.reliablemessage.webflux.ReactiveOutboxStore;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import org.springframework.r2dbc.core.DatabaseClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.*;
import java.util.Map;
import java.util.Objects;

public class R2dbcOutboxStore implements ReactiveOutboxStore {

    private static final Duration PROCESSING_LEASE_DURATION = Duration.ofMinutes(5);
    private static final TypeReference<Map<String, String>> STRING_MAP = new TypeReference<>() {
    };

    private final DatabaseClient databaseClient;
    private final ObjectMapper objectMapper;
    private final Clock clock;

    public R2dbcOutboxStore(DatabaseClient databaseClient, ObjectMapper objectMapper) {
        this(databaseClient, objectMapper, Clock.systemUTC());
    }

    public R2dbcOutboxStore(DatabaseClient databaseClient, ObjectMapper objectMapper, Clock clock) {
        this.databaseClient = Objects.requireNonNull(databaseClient, "databaseClient must not be null");
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
    }

    public Mono<Void> initializeSchema() {
        return databaseClient.sql("""
                        create table if not exists message_outbox (
                            id varchar(64) primary key,
                            event_name varchar(255) not null,
                            aggregate_id varchar(255),
                            idempotency_key varchar(255),
                            partition_key varchar(255),
                            payload text not null,
                            headers text,
                            status varchar(32) not null,
                            retry_count int not null default 0,
                            next_retry_at timestamp,
                            processing_started_at timestamp,
                            created_at timestamp not null,
                            published_at timestamp,
                            last_error text
                        )
                        """)
                .fetch()
                .rowsUpdated()
                .then();
    }

    @Override
    public Mono<Void> save(OutboxMessage message) {
        Objects.requireNonNull(message, "message must not be null");
        DatabaseClient.GenericExecuteSpec spec = databaseClient.sql("""
                        insert into message_outbox
                        (id, event_name, aggregate_id, idempotency_key, partition_key, payload, headers,
                         status, retry_count, next_retry_at, processing_started_at, created_at, published_at, last_error)
                        values (:id, :eventName, :aggregateId, :idempotencyKey, :partitionKey, :payload, :headers,
                                :status, :retryCount, :nextRetryAt, :processingStartedAt, :createdAt, :publishedAt, :lastError)
                        """)
                .bind("id", message.id())
                .bind("eventName", message.eventName())
                .bind("payload", toJson(message.payload()))
                .bind("headers", toJson(message.headers()))
                .bind("status", message.status().name())
                .bind("retryCount", message.retryCount())
                .bind("createdAt", localDateTime(message.createdAt()));

        spec = bindNullable(spec, "aggregateId", message.aggregateId(), String.class);
        spec = bindNullable(spec, "idempotencyKey", message.idempotencyKey(), String.class);
        spec = bindNullable(spec, "partitionKey", message.partitionKey(), String.class);
        spec = bindNullable(spec, "nextRetryAt", localDateTime(message.nextRetryAt()), LocalDateTime.class);
        spec = spec.bindNull("processingStartedAt", LocalDateTime.class);
        spec = bindNullable(spec, "publishedAt", localDateTime(message.publishedAt()), LocalDateTime.class);
        spec = bindNullable(spec, "lastError", message.lastError(), String.class);

        return spec.fetch().rowsUpdated().then();
    }

    @Override
    public Flux<OutboxMessage> findPending(int limit) {
        if (limit <= 0) {
            return Flux.error(new IllegalArgumentException("limit must be positive"));
        }
        Instant now = clock.instant();
        Instant expiredProcessingBefore = now.minus(PROCESSING_LEASE_DURATION);

        return databaseClient.sql("""
                        select id
                        from message_outbox
                        where status = :pending
                           or (status = :failed and (next_retry_at is null or next_retry_at <= :now))
                           or (status = :processing and processing_started_at <= :expiredProcessingBefore)
                        order by created_at asc
                        limit :limit
                        """)
                .bind("pending", MessageStatus.PENDING.name())
                .bind("failed", MessageStatus.FAILED.name())
                .bind("now", localDateTime(now))
                .bind("processing", MessageStatus.PROCESSING.name())
                .bind("expiredProcessingBefore", localDateTime(expiredProcessingBefore))
                .bind("limit", limit)
                .map((row, metadata) -> row.get("id", String.class))
                .all()
                .concatMap(id -> claim(id, now)
                        .filter(Boolean::booleanValue)
                        .flatMap(ignored -> findByIdProcessing(id)));
    }

    @Override
    public Mono<Void> markPublished(String id) {
        requireId(id);
        return databaseClient.sql("""
                        update message_outbox
                        set status = :published, published_at = :publishedAt, last_error = null, processing_started_at = null
                        where id = :id and status = :processing
                        """)
                .bind("published", MessageStatus.PUBLISHED.name())
                .bind("publishedAt", localDateTime(clock.instant()))
                .bind("id", id)
                .bind("processing", MessageStatus.PROCESSING.name())
                .fetch()
                .rowsUpdated()
                .then();
    }

    @Override
    public Mono<Void> markFailed(String id, Throwable error, Instant nextRetryAt) {
        requireId(id);
        DatabaseClient.GenericExecuteSpec spec = databaseClient.sql("""
                        update message_outbox
                        set status = :failed, retry_count = coalesce(retry_count, 0) + 1, next_retry_at = :nextRetryAt,
                            last_error = :lastError, processing_started_at = null
                        where id = :id and status = :processing
                        """)
                .bind("failed", MessageStatus.FAILED.name())
                .bind("id", id)
                .bind("processing", MessageStatus.PROCESSING.name());

        spec = bindNullable(spec, "nextRetryAt", localDateTime(nextRetryAt), LocalDateTime.class);
        spec = bindNullable(spec, "lastError", error == null ? null : error.getMessage(), String.class);
        return spec.fetch().rowsUpdated().then();
    }

    private Mono<Boolean> claim(String id, Instant now) {
        return databaseClient.sql("""
                        update message_outbox
                        set status = :processing, processing_started_at = :processingStartedAt
                        where id = :id
                          and (status = :pending
                            or (status = :failed and (next_retry_at is null or next_retry_at <= :now))
                            or (status = :processing and processing_started_at <= :expiredProcessingBefore))
                        """)
                .bind("processing", MessageStatus.PROCESSING.name())
                .bind("processingStartedAt", localDateTime(now))
                .bind("id", id)
                .bind("pending", MessageStatus.PENDING.name())
                .bind("failed", MessageStatus.FAILED.name())
                .bind("now", localDateTime(now))
                .bind("expiredProcessingBefore", localDateTime(now.minus(PROCESSING_LEASE_DURATION)))
                .fetch()
                .rowsUpdated()
                .map(updated -> updated == 1);
    }

    private Mono<OutboxMessage> findByIdProcessing(String id) {
        return databaseClient.sql("""
                        select id, event_name, aggregate_id, idempotency_key, partition_key, payload, headers,
                               status, retry_count, next_retry_at, created_at, published_at, last_error
                        from message_outbox
                        where id = :id and status = :processing
                        """)
                .bind("id", id)
                .bind("processing", MessageStatus.PROCESSING.name())
                .map(this::mapRow)
                .one();
    }

    private OutboxMessage mapRow(Row row, RowMetadata metadata) {
        return new OutboxMessage(
                row.get("id", String.class),
                row.get("event_name", String.class),
                row.get("aggregate_id", String.class),
                row.get("idempotency_key", String.class),
                row.get("partition_key", String.class),
                readPayload(row.get("payload", String.class)),
                readHeaders(row.get("headers", String.class)),
                MessageStatus.valueOf(row.get("status", String.class)),
                retryCount(row),
                instant(row.get("next_retry_at", LocalDateTime.class)),
                instant(row.get("created_at", LocalDateTime.class)),
                instant(row.get("published_at", LocalDateTime.class)),
                row.get("last_error", String.class)
        );
    }

    private int retryCount(Row row) {
        Integer retryCount = row.get("retry_count", Integer.class);
        return retryCount == null ? 0 : retryCount;
    }

    private JsonNode readPayload(String json) {
        try {
            return objectMapper.readTree(json);
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("Outbox payload is not valid JSON", ex);
        }
    }

    private Map<String, String> readHeaders(String json) {
        if (json == null || json.isBlank()) {
            return Map.of();
        }
        try {
            return objectMapper.readValue(json, STRING_MAP);
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("Outbox headers are not valid JSON", ex);
        }
    }

    private String toJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value == null ? objectMapper.nullNode() : value);
        } catch (JsonProcessingException ex) {
            throw new IllegalArgumentException("value must be JSON serializable", ex);
        }
    }

    private static <T> DatabaseClient.GenericExecuteSpec bindNullable(
            DatabaseClient.GenericExecuteSpec spec,
            String name,
            T value,
            Class<T> type
    ) {
        return value == null ? spec.bindNull(name, type) : spec.bind(name, value);
    }

    private static LocalDateTime localDateTime(Instant instant) {
        return instant == null ? null : LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
    }

    private static Instant instant(LocalDateTime dateTime) {
        return dateTime == null ? null : dateTime.toInstant(ZoneOffset.UTC);
    }

    private static void requireId(String id) {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("id must not be blank");
        }
    }
}
