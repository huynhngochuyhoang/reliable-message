package io.github.huynhngochuyhoang.reliablemessage.idempotency.redis.reactive;

import io.github.huynhngochuyhoang.reliablemessage.webflux.IdempotencyStartResult;
import io.github.huynhngochuyhoang.reliablemessage.webflux.IdempotencyState;
import io.github.huynhngochuyhoang.reliablemessage.webflux.ReactiveIdempotencyStore;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.ReactiveValueOperations;
import reactor.core.publisher.Mono;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

public class ReactiveRedisIdempotencyStore implements ReactiveIdempotencyStore {

    private static final String DEFAULT_PREFIX = "reliable-message:idempotency:";
    private static final Duration FALLBACK_TTL = Duration.ofHours(24);

    private final ReactiveStringRedisTemplate redisTemplate;
    private final String keyPrefix;
    private final Clock clock;

    public ReactiveRedisIdempotencyStore(ReactiveStringRedisTemplate redisTemplate) {
        this(redisTemplate, DEFAULT_PREFIX, Clock.systemUTC());
    }

    public ReactiveRedisIdempotencyStore(ReactiveStringRedisTemplate redisTemplate, String keyPrefix, Clock clock) {
        this.redisTemplate = Objects.requireNonNull(redisTemplate, "redisTemplate must not be null");
        this.keyPrefix = Objects.requireNonNull(keyPrefix, "keyPrefix must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
    }

    @Override
    public Mono<IdempotencyStartResult> tryStart(String key, Duration ttl) {
        requireKey(key);
        requirePositiveTtl(ttl);

        Instant now = clock.instant();
        Instant expiresAt = now.plus(ttl);
        String redisKey = redisKey(key);
        String processingState = encode(IdempotencyState.PROCESSING, expiresAt, null);
        ReactiveValueOperations<String, String> values = redisTemplate.opsForValue();

        return values.setIfAbsent(redisKey, processingState, ttl)
                .flatMap(started -> Boolean.TRUE.equals(started)
                        ? Mono.just(IdempotencyStartResult.startAccepted())
                        : tryStartExisting(values, redisKey, processingState, ttl, now));
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

    private Mono<IdempotencyStartResult> tryStartExisting(
            ReactiveValueOperations<String, String> values,
            String redisKey,
            String processingState,
            Duration ttl,
            Instant now
    ) {
        return values.get(redisKey)
                .flatMap(current -> {
                    StoredState storedState = decode(current);
                    if (storedState != null
                            && storedState.expiresAt().isAfter(now)
                            && storedState.state() != IdempotencyState.FAILED) {
                        return Mono.just(IdempotencyStartResult.duplicate(storedState.state()));
                    }
                    return values.set(redisKey, processingState, ttl)
                            .thenReturn(IdempotencyStartResult.startAccepted());
                })
                .switchIfEmpty(Mono.defer(() -> values.set(redisKey, processingState, ttl)
                        .thenReturn(IdempotencyStartResult.startAccepted())));
    }

    private Mono<Void> updateState(String key, IdempotencyState state, String errorMessage) {
        String redisKey = redisKey(key);
        return redisTemplate.getExpire(redisKey)
                .flatMap(ttl -> {
                    Duration effectiveTtl = effectiveTtl(ttl);
                    if (effectiveTtl == null) {
                        return Mono.empty();
                    }
                    Instant expiresAt = clock.instant().plus(effectiveTtl);
                    return redisTemplate.opsForValue()
                            .set(redisKey, encode(state, expiresAt, errorMessage), effectiveTtl)
                            .then();
                })
                .then();
    }

    private Duration effectiveTtl(Duration ttl) {
        if (ttl == null || ttl.isZero()) {
            return null;
        }
        if (ttl.isNegative()) {
            return FALLBACK_TTL;
        }
        return ttl;
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
