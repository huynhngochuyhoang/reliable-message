package io.github.huynhngochuyhoang.reliablemessage.idempotency.redis;

import io.github.huynhngochuyhoang.reliablemessage.mvc.IdempotencyStartResult;
import io.github.huynhngochuyhoang.reliablemessage.mvc.IdempotencyState;
import io.github.huynhngochuyhoang.reliablemessage.mvc.IdempotencyStore;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class RedisIdempotencyStore implements IdempotencyStore {

    private static final String DEFAULT_PREFIX = "reliable-message:idempotency:";

    private final StringRedisTemplate redisTemplate;
    private final String keyPrefix;
    private final Clock clock;

    public RedisIdempotencyStore(StringRedisTemplate redisTemplate) {
        this(redisTemplate, DEFAULT_PREFIX, Clock.systemUTC());
    }

    public RedisIdempotencyStore(StringRedisTemplate redisTemplate, String keyPrefix, Clock clock) {
        this.redisTemplate = Objects.requireNonNull(redisTemplate, "redisTemplate must not be null");
        this.keyPrefix = Objects.requireNonNull(keyPrefix, "keyPrefix must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
    }

    @Override
    public IdempotencyStartResult tryStart(String key, Duration ttl) {
        requireKey(key);
        requirePositiveTtl(ttl);

        String redisKey = redisKey(key);
        Instant expiresAt = clock.instant().plus(ttl);
        Boolean started = redisTemplate.opsForValue().setIfAbsent(
                redisKey,
                encode(IdempotencyState.PROCESSING, expiresAt, null),
                ttl
        );
        if (Boolean.TRUE.equals(started)) {
            return IdempotencyStartResult.startAccepted();
        }

        StoredState storedState = decode(redisTemplate.opsForValue().get(redisKey));
        if (storedState == null || !storedState.expiresAt().isAfter(clock.instant())) {
            redisTemplate.opsForValue().set(redisKey, encode(IdempotencyState.PROCESSING, expiresAt, null), ttl);
            return IdempotencyStartResult.startAccepted();
        }
        if (storedState.state() == IdempotencyState.FAILED) {
            redisTemplate.opsForValue().set(redisKey, encode(IdempotencyState.PROCESSING, expiresAt, null), ttl);
            return IdempotencyStartResult.startAccepted();
        }

        return IdempotencyStartResult.duplicate(storedState.state());
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
        Duration ttl = ttlMillis == null || ttlMillis <= 0 ? Duration.ofHours(24) : Duration.ofMillis(ttlMillis);
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
        return new StoredState(
                IdempotencyState.valueOf(parts[0]),
                Instant.ofEpochMilli(Long.parseLong(parts[1]))
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

    private record StoredState(
            IdempotencyState state,
            Instant expiresAt
    ) {
    }
}
