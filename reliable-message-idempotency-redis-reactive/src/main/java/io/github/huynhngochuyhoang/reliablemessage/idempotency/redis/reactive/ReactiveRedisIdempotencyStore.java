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
import java.util.UUID;

public class ReactiveRedisIdempotencyStore implements ReactiveIdempotencyStore {

    private static final String DEFAULT_PREFIX = "reliable-message:idempotency:";
    private static final Duration FALLBACK_TTL = Duration.ofHours(24);
    private static final String RESTART_LOCK_SUFFIX = ":restart-lock";
    private static final Duration RESTART_LOCK_TTL = Duration.ofSeconds(10);

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
                        return Mono.just(IdempotencyStartResult.duplicate(duplicateState(storedState)));
                    }
                    return tryRestartWithLock(redisKey, processingState, ttl, now)
                            .flatMap(restarted -> Boolean.TRUE.equals(restarted)
                                    ? Mono.just(IdempotencyStartResult.startAccepted())
                                    : Mono.just(IdempotencyStartResult.duplicate(duplicateState(storedState))));
                })
                .switchIfEmpty(Mono.defer(() -> tryRestartWithLock(redisKey, processingState, ttl, now)
                        .map(restarted -> Boolean.TRUE.equals(restarted)
                                ? IdempotencyStartResult.startAccepted()
                                : IdempotencyStartResult.duplicate(IdempotencyState.PROCESSING))));
    }

    private Mono<Boolean> tryRestartWithLock(String redisKey, String processingState, Duration ttl, Instant now) {
        ReactiveValueOperations<String, String> values = redisTemplate.opsForValue();
        String lockKey = redisKey + RESTART_LOCK_SUFFIX;
        String lockToken = UUID.randomUUID().toString();

        return values.setIfAbsent(lockKey, lockToken, RESTART_LOCK_TTL)
                .flatMap(locked -> Boolean.TRUE.equals(locked)
                        ? restartWithLock(values, redisKey, processingState, ttl, now)
                        .flatMap(restarted -> releaseRestartLock(values, lockKey, lockToken).thenReturn(restarted))
                        .onErrorResume(error -> releaseRestartLock(values, lockKey, lockToken).then(Mono.error(error)))
                        : Mono.just(false));
    }

    private Mono<Boolean> restartWithLock(
            ReactiveValueOperations<String, String> values,
            String redisKey,
            String processingState,
            Duration ttl,
            Instant now
    ) {
        return values.get(redisKey)
                .flatMap(current -> isRetryable(current, now)
                        ? values.set(redisKey, processingState, ttl).thenReturn(true)
                        : Mono.just(false))
                .switchIfEmpty(values.set(redisKey, processingState, ttl).thenReturn(true));
    }

    private Mono<Void> releaseRestartLock(ReactiveValueOperations<String, String> values, String lockKey, String lockToken) {
        return values.get(lockKey)
                .filter(lockToken::equals)
                .flatMap(ignored -> redisTemplate.delete(lockKey))
                .then();
    }

    private static boolean isRetryable(String current, Instant now) {
        StoredState storedState = decode(current);
        return storedState == null
                || storedState.state() == IdempotencyState.FAILED
                || !storedState.expiresAt().isAfter(now);
    }

    private static IdempotencyState duplicateState(StoredState storedState) {
        return storedState == null ? IdempotencyState.PROCESSING : storedState.state();
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
