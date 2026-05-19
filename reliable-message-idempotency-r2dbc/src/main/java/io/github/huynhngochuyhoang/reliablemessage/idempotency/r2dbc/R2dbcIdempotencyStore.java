package io.github.huynhngochuyhoang.reliablemessage.idempotency.r2dbc;

import io.github.huynhngochuyhoang.reliablemessage.webflux.IdempotencyStartResult;
import io.github.huynhngochuyhoang.reliablemessage.webflux.IdempotencyState;
import io.github.huynhngochuyhoang.reliablemessage.webflux.ReactiveIdempotencyStore;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.r2dbc.core.DatabaseClient;
import reactor.core.publisher.Mono;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Objects;

public class R2dbcIdempotencyStore implements ReactiveIdempotencyStore {

    private final DatabaseClient databaseClient;
    private final Clock clock;

    public R2dbcIdempotencyStore(DatabaseClient databaseClient) {
        this(databaseClient, Clock.systemUTC());
    }

    public R2dbcIdempotencyStore(DatabaseClient databaseClient, Clock clock) {
        this.databaseClient = Objects.requireNonNull(databaseClient, "databaseClient must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
    }

    public Mono<Void> initializeSchema() {
        return databaseClient.sql("""
                        create table if not exists message_idempotency (
                            idempotency_key varchar(255) primary key,
                            status varchar(32) not null,
                            expires_at timestamp not null,
                            created_at timestamp not null,
                            updated_at timestamp not null,
                            last_error text
                        )
                        """)
                .fetch()
                .rowsUpdated()
                .then();
    }

    @Override
    public Mono<IdempotencyStartResult> tryStart(String key, Duration ttl) {
        requireKey(key);
        requirePositiveTtl(ttl);

        Instant now = clock.instant();
        Instant expiresAt = now.plus(ttl);
        return databaseClient.sql("""
                        insert into message_idempotency
                        (idempotency_key, status, expires_at, created_at, updated_at, last_error)
                        values (:key, :status, :expiresAt, :createdAt, :updatedAt, null)
                        """)
                .bind("key", key)
                .bind("status", IdempotencyState.PROCESSING.name())
                .bind("expiresAt", localDateTime(expiresAt))
                .bind("createdAt", localDateTime(now))
                .bind("updatedAt", localDateTime(now))
                .fetch()
                .rowsUpdated()
                .thenReturn(IdempotencyStartResult.startAccepted())
                .onErrorResume(DuplicateKeyException.class, ignored -> tryRestartExisting(key, now, expiresAt));
    }

    @Override
    public Mono<Void> markSuccess(String key) {
        requireKey(key);
        return updateState(key, IdempotencyState.SUCCESS, null);
    }

    @Override
    public Mono<Void> markFailed(String key, Throwable error) {
        requireKey(key);
        return updateState(key, IdempotencyState.FAILED, error == null ? null : error.getMessage());
    }

    private Mono<IdempotencyStartResult> tryRestartExisting(String key, Instant now, Instant expiresAt) {
        return databaseClient.sql("""
                        update message_idempotency
                        set status = :processing, expires_at = :expiresAt, updated_at = :updatedAt, last_error = null
                        where idempotency_key = :key
                          and (status = :failed or expires_at <= :now)
                        """)
                .bind("processing", IdempotencyState.PROCESSING.name())
                .bind("expiresAt", localDateTime(expiresAt))
                .bind("updatedAt", localDateTime(now))
                .bind("key", key)
                .bind("failed", IdempotencyState.FAILED.name())
                .bind("now", localDateTime(now))
                .fetch()
                .rowsUpdated()
                .flatMap(updated -> updated == 1
                        ? Mono.just(IdempotencyStartResult.startAccepted())
                        : findState(key, now).map(IdempotencyStartResult::duplicate));
    }

    private Mono<IdempotencyState> findState(String key, Instant now) {
        return databaseClient.sql("""
                        select status, expires_at
                        from message_idempotency
                        where idempotency_key = :key
                        """)
                .bind("key", key)
                .map((row, metadata) -> {
                    Instant expiresAt = instant(row.get("expires_at", LocalDateTime.class));
                    if (expiresAt == null || !expiresAt.isAfter(now)) {
                        return IdempotencyState.EXPIRED;
                    }
                    return IdempotencyState.valueOf(row.get("status", String.class));
                })
                .one()
                .defaultIfEmpty(IdempotencyState.EXPIRED);
    }

    private Mono<Void> updateState(String key, IdempotencyState state, String errorMessage) {
        DatabaseClient.GenericExecuteSpec spec = databaseClient.sql("""
                        update message_idempotency
                        set status = :status, updated_at = :updatedAt, last_error = :lastError
                        where idempotency_key = :key
                        """)
                .bind("status", state.name())
                .bind("updatedAt", localDateTime(clock.instant()))
                .bind("key", key);
        spec = errorMessage == null ? spec.bindNull("lastError", String.class) : spec.bind("lastError", errorMessage);
        return spec.fetch().rowsUpdated().then();
    }

    private static LocalDateTime localDateTime(Instant instant) {
        return instant == null ? null : LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
    }

    private static Instant instant(LocalDateTime dateTime) {
        return dateTime == null ? null : dateTime.toInstant(ZoneOffset.UTC);
    }

    private static void requireKey(String key) {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("key must not be blank");
        }
    }

    private static void requirePositiveTtl(Duration ttl) {
        if (ttl == null || ttl.isZero() || ttl.isNegative()) {
            throw new IllegalArgumentException("ttl must be positive");
        }
    }
}
