package io.github.huynhngochuyhoang.reliablemessage.idempotency.jdbc;

import io.github.huynhngochuyhoang.reliablemessage.mvc.IdempotencyStartResult;
import io.github.huynhngochuyhoang.reliablemessage.mvc.IdempotencyState;
import io.github.huynhngochuyhoang.reliablemessage.mvc.IdempotencyStore;
import io.github.huynhngochuyhoang.reliablemessage.observability.MessageObservability;
import io.github.huynhngochuyhoang.reliablemessage.observability.MessageTags;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.observation.ObservationRegistry;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

public class JdbcIdempotencyStore implements IdempotencyStore {

    private final JdbcTemplate jdbcTemplate;
    private final Clock clock;
    private final MessageObservability observability;

    public JdbcIdempotencyStore(JdbcTemplate jdbcTemplate) {
        this(jdbcTemplate, Clock.systemUTC());
    }

    public JdbcIdempotencyStore(JdbcTemplate jdbcTemplate, Clock clock) {
        this(jdbcTemplate, clock, new MessageObservability(new SimpleMeterRegistry(), ObservationRegistry.NOOP));
    }

    public JdbcIdempotencyStore(JdbcTemplate jdbcTemplate, Clock clock, MessageObservability observability) {
        this.jdbcTemplate = Objects.requireNonNull(jdbcTemplate, "jdbcTemplate must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
        this.observability = Objects.requireNonNull(observability, "observability must not be null");
    }

    public void initializeSchema() {
        jdbcTemplate.execute("""
                create table if not exists message_idempotency (
                    idempotency_key varchar(255) primary key,
                    status varchar(32) not null,
                    expires_at timestamp not null,
                    created_at timestamp not null,
                    updated_at timestamp not null,
                    last_error text
                )
                """);
    }

    @Override
    public IdempotencyStartResult tryStart(String key, Duration ttl) {
        return observability.observe(
                "message.idempotency.check",
                "message_idempotency_check_duration",
                new MessageTags("mvc", "idempotency", "idempotency", null, "check"),
                () -> tryStartInternal(key, ttl)
        );
    }

    @Override
    public void markSuccess(String key) {
        requireKey(key);
        updateState(key, IdempotencyState.SUCCESS, null);
    }

    @Override
    public void markFailed(String key, Throwable error) {
        requireKey(key);
        updateState(key, IdempotencyState.FAILED, error == null ? null : error.getMessage());
    }

    private IdempotencyStartResult tryRestartExisting(String key, Instant now, Instant expiresAt) {
        int updated = jdbcTemplate.update("""
                        update message_idempotency
                        set status = ?, expires_at = ?, updated_at = ?, last_error = null
                        where idempotency_key = ?
                          and (status = ? or expires_at <= ?)
                        """,
                IdempotencyState.PROCESSING.name(),
                Timestamp.from(expiresAt),
                Timestamp.from(now),
                key,
                IdempotencyState.FAILED.name(),
                Timestamp.from(now)
        );
        if (updated == 1) {
            return IdempotencyStartResult.startAccepted();
        }

        IdempotencyState state = findState(key, now);
        return IdempotencyStartResult.duplicate(state);
    }

    private IdempotencyStartResult tryStartInternal(String key, Duration ttl) {
        requireKey(key);
        requirePositiveTtl(ttl);

        Instant now = clock.instant();
        Instant expiresAt = now.plus(ttl);
        try {
            jdbcTemplate.update("""
                            insert into message_idempotency
                            (idempotency_key, status, expires_at, created_at, updated_at, last_error)
                            values (?, ?, ?, ?, ?, null)
                            """,
                    key,
                    IdempotencyState.PROCESSING.name(),
                    Timestamp.from(expiresAt),
                    Timestamp.from(now),
                    Timestamp.from(now)
            );
            return IdempotencyStartResult.startAccepted();
        } catch (DuplicateKeyException ignored) {
            IdempotencyStartResult result = tryRestartExisting(key, now, expiresAt);
            if (!result.started()) {
                observability.increment("message_duplicate_total",
                        new MessageTags("mvc", "idempotency", "idempotency", null, "duplicate"));
            }
            return result;
        }
    }

    private IdempotencyState findState(String key, Instant now) {
        List<IdempotencyState> states = jdbcTemplate.query("""
                        select status, expires_at
                        from message_idempotency
                        where idempotency_key = ?
                        """,
                (rs, rowNum) -> {
                    Instant expiresAt = rs.getTimestamp("expires_at").toInstant();
                    if (!expiresAt.isAfter(now)) {
                        return IdempotencyState.EXPIRED;
                    }
                    return IdempotencyState.valueOf(rs.getString("status"));
                },
                key
        );
        return states.isEmpty() ? IdempotencyState.EXPIRED : states.getFirst();
    }

    private void updateState(String key, IdempotencyState state, String errorMessage) {
        jdbcTemplate.update("""
                        update message_idempotency
                        set status = ?, updated_at = ?, last_error = ?
                        where idempotency_key = ?
                        """,
                state.name(),
                Timestamp.from(clock.instant()),
                errorMessage,
                key
        );
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
