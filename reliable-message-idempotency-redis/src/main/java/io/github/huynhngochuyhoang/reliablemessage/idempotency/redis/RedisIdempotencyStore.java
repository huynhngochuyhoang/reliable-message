package io.github.huynhngochuyhoang.reliablemessage.idempotency.redis;

import io.github.huynhngochuyhoang.reliablemessage.mvc.IdempotencyStartResult;
import io.github.huynhngochuyhoang.reliablemessage.mvc.IdempotencyState;
import io.github.huynhngochuyhoang.reliablemessage.mvc.IdempotencyStore;
import io.github.huynhngochuyhoang.reliablemessage.observability.MessageObservability;
import io.github.huynhngochuyhoang.reliablemessage.observability.MessageTags;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.observation.ObservationRegistry;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class RedisIdempotencyStore implements IdempotencyStore {

    private static final String DEFAULT_PREFIX = "reliable-message:idempotency:";
    private static final int MAX_START_ATTEMPTS = 3;

    private final StringRedisTemplate redisTemplate;
    private final String keyPrefix;
    private final Clock clock;
    private final MessageObservability observability;

    public RedisIdempotencyStore(StringRedisTemplate redisTemplate) {
        this(redisTemplate, DEFAULT_PREFIX, Clock.systemUTC());
    }

    public RedisIdempotencyStore(StringRedisTemplate redisTemplate, String keyPrefix, Clock clock) {
        this(redisTemplate, keyPrefix, clock, new MessageObservability(new SimpleMeterRegistry(), ObservationRegistry.NOOP));
    }

    public RedisIdempotencyStore(
            StringRedisTemplate redisTemplate,
            String keyPrefix,
            Clock clock,
            MessageObservability observability
    ) {
        this.redisTemplate = Objects.requireNonNull(redisTemplate, "redisTemplate must not be null");
        this.keyPrefix = Objects.requireNonNull(keyPrefix, "keyPrefix must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
        this.observability = Objects.requireNonNull(observability, "observability must not be null");
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

    private IdempotencyStartResult tryStartInternal(String key, Duration ttl) {
        requireKey(key);
        requirePositiveTtl(ttl);

        Instant now = clock.instant();
        Instant expiresAt = now.plus(ttl);
        String redisKey = redisKey(key);
        String processingState = encode(IdempotencyState.PROCESSING, expiresAt, null);

        for (int attempt = 0; attempt < MAX_START_ATTEMPTS; attempt++) {
            IdempotencyStartResult result = tryStartOnce(redisKey, processingState, ttl, now);
            if (result != null) {
                if (!result.started()) {
                    observability.increment("message_duplicate_total",
                            new MessageTags("mvc", "idempotency", "idempotency", null, "duplicate"));
                }
                return result;
            }
        }
        throw new IllegalStateException("Failed to start Redis idempotency key after concurrent updates");
    }

    private IdempotencyStartResult tryStartOnce(String redisKey, String processingState, Duration ttl, Instant now) {
        return redisTemplate.execute(new SessionCallback<>() {
            @Override
            @SuppressWarnings({"unchecked", "rawtypes"})
            public IdempotencyStartResult execute(RedisOperations operations) {
                operations.watch(redisKey);
                String current = (String) operations.opsForValue().get(redisKey);
                StoredState storedState = decode(current);
                if (storedState != null
                        && storedState.expiresAt().isAfter(now)
                        && storedState.state() != IdempotencyState.FAILED) {
                    operations.unwatch();
                    return IdempotencyStartResult.duplicate(storedState.state());
                }

                operations.multi();
                operations.opsForValue().set(redisKey, processingState, ttl);
                List<Object> result = operations.exec();
                return result == null ? null : IdempotencyStartResult.startAccepted();
            }
        });
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

    private void updateState(String key, IdempotencyState state, String errorMessage) {
        String redisKey = redisKey(key);
        Long ttlMillis = redisTemplate.getExpire(redisKey, TimeUnit.MILLISECONDS);
        if (ttlMillis == null || ttlMillis == -2) {
            return;
        }
        Duration ttl = ttlMillis == -1 ? Duration.ofHours(24) : Duration.ofMillis(ttlMillis);
        Instant expiresAt = clock.instant().plus(ttl);
        redisTemplate.opsForValue().set(redisKey, encode(state, expiresAt, errorMessage), ttl);
    }

    private String redisKey(String key) {
        return keyPrefix + key;
    }

    private static String encode(IdempotencyState state, Instant expiresAt, String errorMessage) {
        String error = errorMessage == null ? "" : errorMessage.replace("|", " ");
        return state.name() + "|" + expiresAt.toEpochMilli() + "|" + error;
    }

    private static StoredState decode(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        String[] parts = value.split("\\|", 3);
        if (parts.length < 2) {
            return null;
        }
        try {
            return new StoredState(
                    IdempotencyState.valueOf(parts[0]),
                    Instant.ofEpochMilli(Long.parseLong(parts[1]))
            );
        } catch (IllegalArgumentException error) {
            return null;
        }
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

    private record StoredState(
            IdempotencyState state,
            Instant expiresAt
    ) {
    }
}
