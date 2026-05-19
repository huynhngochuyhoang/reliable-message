package io.github.huynhngochuyhoang.reliablemessage.outbox.jdbc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.huynhngochuyhoang.reliablemessage.core.MessageStatus;
import io.github.huynhngochuyhoang.reliablemessage.mvc.OutboxMessage;
import io.github.huynhngochuyhoang.reliablemessage.mvc.OutboxStore;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class JdbcOutboxStore implements OutboxStore {

    private static final Duration PROCESSING_LEASE_DURATION = Duration.ofMinutes(5);

    private static final TypeReference<Map<String, String>> STRING_MAP = new TypeReference<>() {
    };

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;
    private final Clock clock;

    public JdbcOutboxStore(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper) {
        this(jdbcTemplate, objectMapper, Clock.systemUTC());
    }

    public JdbcOutboxStore(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper, Clock clock) {
        this.jdbcTemplate = Objects.requireNonNull(jdbcTemplate, "jdbcTemplate must not be null");
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
    }

    public void initializeSchema() {
        jdbcTemplate.execute("""
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
                """);
    }

    @Override
    public void save(OutboxMessage message) {
        Objects.requireNonNull(message, "message must not be null");
        jdbcTemplate.update("""
                        insert into message_outbox
                        (id, event_name, aggregate_id, idempotency_key, partition_key, payload, headers,
                         status, retry_count, next_retry_at, processing_started_at, created_at, published_at, last_error)
                        values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                message.id(),
                message.eventName(),
                message.aggregateId(),
                message.idempotencyKey(),
                message.partitionKey(),
                toJson(message.payload()),
                toJson(message.headers()),
                message.status().name(),
                message.retryCount(),
                timestamp(message.nextRetryAt()),
                null,
                Timestamp.from(message.createdAt()),
                timestamp(message.publishedAt()),
                message.lastError()
        );
    }

    @Override
    public List<OutboxMessage> findPending(int limit) {
        if (limit <= 0) {
            throw new IllegalArgumentException("limit must be positive");
        }
        Instant now = clock.instant();
        Instant expiredProcessingBefore = now.minus(PROCESSING_LEASE_DURATION);
        List<String> candidateIds = jdbcTemplate.queryForList("""
                        select id
                        from message_outbox
                        where status = ?
                           or (status = ? and (next_retry_at is null or next_retry_at <= ?))
                           or (status = ? and processing_started_at <= ?)
                        order by created_at asc
                        limit ?
                        """,
                String.class,
                MessageStatus.PENDING.name(),
                MessageStatus.FAILED.name(),
                Timestamp.from(now),
                MessageStatus.PROCESSING.name(),
                Timestamp.from(expiredProcessingBefore),
                limit
        );
        return candidateIds.stream()
                .filter(id -> claim(id, now))
                .map(this::findById)
                .toList();
    }


    @Override
    public List<OutboxMessage> findForAdmin(int limit) {
        if (limit <= 0) {
            throw new IllegalArgumentException("limit must be positive");
        }
        Instant now = clock.instant();
        Instant expiredProcessingBefore = now.minus(PROCESSING_LEASE_DURATION);
        return jdbcTemplate.query("""
                        select id, event_name, aggregate_id, idempotency_key, partition_key, payload, headers,
                               status, retry_count, next_retry_at, created_at, published_at, last_error
                        from message_outbox
                        where status = ?
                           or (status = ? and (next_retry_at is null or next_retry_at <= ?))
                           or (status = ? and processing_started_at <= ?)
                        order by created_at asc
                        limit ?
                        """,
                this::mapRow,
                MessageStatus.PENDING.name(),
                MessageStatus.FAILED.name(),
                Timestamp.from(now),
                MessageStatus.PROCESSING.name(),
                Timestamp.from(expiredProcessingBefore),
                limit
        );
    }

    @Override
    public void markPublished(String id) {
        requireId(id);
        jdbcTemplate.update("""
                        update message_outbox
                        set status = ?, published_at = ?, last_error = null, processing_started_at = null
                        where id = ? and status = ?
                        """,
                MessageStatus.PUBLISHED.name(),
                Timestamp.from(clock.instant()),
                id
        );
    }

    @Override
    public void markFailed(String id, Throwable error, Instant nextRetryAt) {
        requireId(id);
        jdbcTemplate.update("""
                        update message_outbox
                        set status = ?, retry_count = retry_count + 1, next_retry_at = ?, last_error = ?, processing_started_at = null
                        where id = ? and status = ?
                        """,
                MessageStatus.FAILED.name(),
                timestamp(nextRetryAt),
                error == null ? null : error.getMessage(),
                id,
                MessageStatus.PROCESSING.name()
        );
    }

    private boolean claim(String id, Instant now) {
        int updated = jdbcTemplate.update("""
                        update message_outbox
                        set status = ?, processing_started_at = ?
                        where id = ?
                          and (status = ?
                            or (status = ? and (next_retry_at is null or next_retry_at <= ?))
                            or (status = ? and processing_started_at <= ?))
                        """,
                MessageStatus.PROCESSING.name(),
                Timestamp.from(now),
                id,
                MessageStatus.PENDING.name(),
                MessageStatus.FAILED.name(),
                Timestamp.from(now),
                MessageStatus.PROCESSING.name(),
                Timestamp.from(now.minus(PROCESSING_LEASE_DURATION))
        );
        return updated == 1;
    }

    private OutboxMessage findById(String id) {
        return jdbcTemplate.queryForObject("""
                        select id, event_name, aggregate_id, idempotency_key, partition_key, payload, headers,
                               status, retry_count, next_retry_at, created_at, published_at, last_error
                        from message_outbox
                        where id = ? and status = ?
                        """,
                this::mapRow,
                id
        );
    }

    private OutboxMessage mapRow(ResultSet rs, int rowNum) throws SQLException {
        return new OutboxMessage(
                rs.getString("id"),
                rs.getString("event_name"),
                rs.getString("aggregate_id"),
                rs.getString("idempotency_key"),
                rs.getString("partition_key"),
                readPayload(rs.getString("payload")),
                readHeaders(rs.getString("headers")),
                MessageStatus.valueOf(rs.getString("status")),
                rs.getInt("retry_count"),
                instant(rs.getTimestamp("next_retry_at")),
                instant(rs.getTimestamp("created_at")),
                instant(rs.getTimestamp("published_at")),
                rs.getString("last_error")
        );
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

    private static Timestamp timestamp(Instant instant) {
        return instant == null ? null : Timestamp.from(instant);
    }

    private static Instant instant(Timestamp timestamp) {
        return timestamp == null ? null : timestamp.toInstant();
    }

    private static void requireId(String id) {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("id must not be blank");
        }
    }
}
