package io.github.huynhngochuyhoang.reliablemessage.outbox.r2dbc;

import io.github.huynhngochuyhoang.reliablemessage.core.MessageStatus;
import io.github.huynhngochuyhoang.reliablemessage.webflux.OutboxMessage;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import org.springframework.r2dbc.core.DatabaseClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Objects;
import java.util.function.BiFunction;

public class GenericConditionalUpdateOutboxClaimStrategy implements OutboxClaimStrategy {

    private static final Duration PROCESSING_LEASE_DURATION = Duration.ofMinutes(5);

    private final DatabaseClient databaseClient;
    private final BiFunction<Row, RowMetadata, OutboxMessage> rowMapper;

    public GenericConditionalUpdateOutboxClaimStrategy(
            DatabaseClient databaseClient,
            BiFunction<Row, RowMetadata, OutboxMessage> rowMapper
    ) {
        this.databaseClient = Objects.requireNonNull(databaseClient, "databaseClient must not be null");
        this.rowMapper = Objects.requireNonNull(rowMapper, "rowMapper must not be null");
    }

    @Override
    public Flux<OutboxMessage> claim(int limit, Instant now) {
        if (limit <= 0) {
            return Flux.error(new IllegalArgumentException("limit must be positive"));
        }
        Objects.requireNonNull(now, "now must not be null");
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
                .concatMap(id -> claimOne(id, now)
                        .filter(Boolean::booleanValue)
                        .flatMap(ignored -> findByIdProcessing(id)));
    }

    private Mono<Boolean> claimOne(String id, Instant now) {
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
                .map(rowMapper::apply)
                .one();
    }

    private static LocalDateTime localDateTime(Instant instant) {
        return LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
    }
}
