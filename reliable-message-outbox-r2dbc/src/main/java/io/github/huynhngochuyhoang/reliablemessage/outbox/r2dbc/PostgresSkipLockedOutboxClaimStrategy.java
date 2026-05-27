package io.github.huynhngochuyhoang.reliablemessage.outbox.r2dbc;

import io.github.huynhngochuyhoang.reliablemessage.core.MessageStatus;
import io.github.huynhngochuyhoang.reliablemessage.webflux.OutboxMessage;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import org.springframework.r2dbc.core.DatabaseClient;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Objects;
import java.util.function.BiFunction;

public class PostgresSkipLockedOutboxClaimStrategy implements OutboxClaimStrategy {

    private static final Duration PROCESSING_LEASE_DURATION = Duration.ofMinutes(5);

    private final DatabaseClient databaseClient;
    private final BiFunction<Row, RowMetadata, OutboxMessage> rowMapper;

    public PostgresSkipLockedOutboxClaimStrategy(
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

        return databaseClient.sql(claimSql())
                .bind("pending", MessageStatus.PENDING.name())
                .bind("failed", MessageStatus.FAILED.name())
                .bind("now", localDateTime(now))
                .bind("processing", MessageStatus.PROCESSING.name())
                .bind("expiredProcessingBefore", localDateTime(now.minus(PROCESSING_LEASE_DURATION)))
                .bind("processingStartedAt", localDateTime(now))
                .bind("limit", limit)
                .map(rowMapper::apply)
                .all();
    }

    static String claimSql() {
        return """
                with claimed as (
                    select id, created_at as claimed_created_at
                    from message_outbox
                    where status = :pending
                       or (status = :failed and (next_retry_at is null or next_retry_at <= :now))
                       or (status = :processing and processing_started_at <= :expiredProcessingBefore)
                    order by created_at asc
                    for update skip locked
                    limit :limit
                ), updated as (
                    update message_outbox mo
                    set status = :processing,
                        processing_started_at = :processingStartedAt
                    from claimed
                    where mo.id = claimed.id
                    returning mo.id, mo.event_name, mo.aggregate_id, mo.idempotency_key, mo.partition_key,
                              mo.payload, mo.headers, mo.status, mo.retry_count, mo.next_retry_at,
                              mo.created_at, mo.published_at, mo.last_error
                )
                select updated.id as id, updated.event_name as event_name,
                       updated.aggregate_id as aggregate_id, updated.idempotency_key as idempotency_key,
                       updated.partition_key as partition_key, updated.payload as payload, updated.headers as headers,
                       updated.status as status, updated.retry_count as retry_count,
                       updated.next_retry_at as next_retry_at, updated.created_at as created_at,
                       updated.published_at as published_at, updated.last_error as last_error,
                       claimed.claimed_created_at as claimed_created_at
                from updated
                join claimed on claimed.id = updated.id
                order by claimed_created_at asc
                """;
    }

    private static LocalDateTime localDateTime(Instant instant) {
        return LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
    }
}
