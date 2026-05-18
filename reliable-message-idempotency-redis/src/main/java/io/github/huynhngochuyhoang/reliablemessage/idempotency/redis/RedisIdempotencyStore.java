package io.github.huynhngochuyhoang.reliablemessage.idempotency.redis;

import io.github.huynhngochuyhoang.reliablemessage.mvc.IdempotencyStartResult;
import io.github.huynhngochuyhoang.reliablemessage.mvc.IdempotencyState;
import io.github.huynhngochuyhoang.reliablemessage.mvc.IdempotencyStore;
import io.github.huynhngochuyhoang.reliablemessage.observability.MessageObservability;
import io.github.huynhngochuyhoang.reliablemessage.observability.MessageTags;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.observation.ObservationRegistry;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class RedisIdempotencyStore implements IdempotencyStore {

    private static final String DEFAULT_PREFIX = "reliable-message:idempotency:";
    private static final String STARTED = "STARTED";
    private static final RedisScript<String> TRY_START_SCRIPT = new DefaultRedisScript<>("""
            local current = redis.call('GET', KEYS[1])
            if not current then
                redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
                return 'STARTED'
            end
            local first_separator = string.find(current, '|')
            local state = ''
            local expires_at = 0
            if first_separator then
                state = string.sub(current, 1, first_separator - 1)
                local rest = string.sub(current, first_separator + 1)
                local second_separator = string.find(rest, '|')
                if second_separator then
                    expires_at = tonumber(string.sub(rest, 1, second_separator - 1)) or 0
                else
                    expires_at = tonumber(rest) or 0
                end
            end
            if expires_at <= tonumber(ARGV[3]) or state == 'FAILED' then
                redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
                return 'STARTED'
            end
            return current
            """, String.class);

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
        String result = redisTemplate.execute(
                TRY_START_SCRIPT,
                List.of(redisKey(key)),
                encode(IdempotencyState.PROCESSING, expiresAt, null),
                String.valueOf(ttl.toMillis()),
                String.valueOf(now.toEpochMilli())
        );
        if (STARTED.equals(result)) {
            return IdempotencyStartResult.startAccepted();
        }

        StoredState storedState = decode(result);
        IdempotencyStartResult duplicate = IdempotencyStartResult.duplicate(
                storedState == null ? IdempotencyState.EXPIRED : storedState.state()
        );
        observability.increment("message_duplicate_total",
                new MessageTags("mvc", "idempotency", "idempotency", null, "duplicate"));
        return duplicate;
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
