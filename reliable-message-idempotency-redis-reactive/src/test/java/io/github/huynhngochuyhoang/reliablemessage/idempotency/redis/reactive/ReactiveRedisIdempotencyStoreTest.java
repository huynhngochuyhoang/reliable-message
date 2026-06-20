package io.github.huynhngochuyhoang.reliablemessage.idempotency.redis.reactive;

import io.github.huynhngochuyhoang.reliablemessage.webflux.IdempotencyState;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.ReactiveValueOperations;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class ReactiveRedisIdempotencyStoreTest {

    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-05-18T00:00:00Z"), ZoneOffset.UTC);

    @Test
    void startsMissingKeyWithSetIfAbsent() {
        RedisMocks redis = redis();
        when(redis.values.setIfAbsent("test:event-1", "PROCESSING|1779062700000|", Duration.ofMinutes(5)))
                .thenReturn(Mono.just(true));
        ReactiveRedisIdempotencyStore store = new ReactiveRedisIdempotencyStore(redis.template, "test:", CLOCK);

        StepVerifier.create(store.tryStart("event-1", Duration.ofMinutes(5)))
                .expectNextMatches(result -> result.started() && result.state() == IdempotencyState.PROCESSING)
                .verifyComplete();
    }

    @Test
    void existingSuccessStateIsDuplicate() {
        RedisMocks redis = redis();
        when(redis.values.setIfAbsent("test:event-1", "PROCESSING|1779062700000|", Duration.ofMinutes(5)))
                .thenReturn(Mono.just(false));
        when(redis.values.get("test:event-1")).thenReturn(Mono.just("SUCCESS|1779062700000|"));
        ReactiveRedisIdempotencyStore store = new ReactiveRedisIdempotencyStore(redis.template, "test:", CLOCK);

        StepVerifier.create(store.tryStart("event-1", Duration.ofMinutes(5)))
                .expectNextMatches(result -> !result.started() && result.state() == IdempotencyState.SUCCESS)
                .verifyComplete();

        verify(redis.values, never()).set("test:event-1", "PROCESSING|1779062700000|", Duration.ofMinutes(5));
    }

    @Test
    void failedStateCanStartAgain() {
        RedisMocks redis = redis();
        when(redis.values.setIfAbsent("test:event-1", "PROCESSING|1779062700000|", Duration.ofMinutes(5)))
                .thenReturn(Mono.just(false));
        when(redis.values.get("test:event-1")).thenReturn(Mono.just("FAILED|1779062700000|boom"));
        when(redis.values.setIfAbsent(eq("test:event-1:restart-lock"), anyString(), eq(Duration.ofSeconds(10))))
                .thenReturn(Mono.just(true));
        when(redis.values.set("test:event-1", "PROCESSING|1779062700000|", Duration.ofMinutes(5)))
                .thenReturn(Mono.just(true));
        ReactiveRedisIdempotencyStore store = new ReactiveRedisIdempotencyStore(redis.template, "test:", CLOCK);

        StepVerifier.create(store.tryStart("event-1", Duration.ofMinutes(5)))
                .expectNextMatches(result -> result.started() && result.state() == IdempotencyState.PROCESSING)
                .verifyComplete();

        verify(redis.values, never()).get("test:event-1:restart-lock");
    }

    @Test
    void malformedStateReturnsDuplicateWhenRestartLockIsHeld() {
        RedisMocks redis = redis();
        when(redis.values.setIfAbsent("test:event-1", "PROCESSING|1779062700000|", Duration.ofMinutes(5)))
                .thenReturn(Mono.just(false));
        when(redis.values.get("test:event-1")).thenReturn(Mono.just("not-a-valid-state"));
        when(redis.values.setIfAbsent(eq("test:event-1:restart-lock"), anyString(), eq(Duration.ofSeconds(10))))
                .thenReturn(Mono.just(false));
        ReactiveRedisIdempotencyStore store = new ReactiveRedisIdempotencyStore(redis.template, "test:", CLOCK);

        StepVerifier.create(store.tryStart("event-1", Duration.ofMinutes(5)))
                .expectNextMatches(result -> !result.started() && result.state() == IdempotencyState.PROCESSING)
                .verifyComplete();
    }

    @Test
    void markSuccessPreservesRemainingTtlAndWritesSuccessState() {
        RedisMocks redis = redis();
        when(redis.template.getExpire("test:event-1")).thenReturn(Mono.just(Duration.ofMinutes(1)));
        when(redis.values.set("test:event-1", "SUCCESS|1779062460000|", Duration.ofMinutes(1)))
                .thenReturn(Mono.just(true));
        ReactiveRedisIdempotencyStore store = new ReactiveRedisIdempotencyStore(redis.template, "test:", CLOCK);

        StepVerifier.create(store.markSuccess("event-1"))
                .verifyComplete();

        verify(redis.values).set("test:event-1", "SUCCESS|1779062460000|", Duration.ofMinutes(1));
    }

    @Test
    void markFailedPreservesRemainingTtlAndWritesFailedState() {
        RedisMocks redis = redis();
        when(redis.template.getExpire("test:event-1")).thenReturn(Mono.just(Duration.ofMinutes(1)));
        when(redis.values.set("test:event-1", "FAILED|1779062460000|boom pipe", Duration.ofMinutes(1)))
                .thenReturn(Mono.just(true));
        ReactiveRedisIdempotencyStore store = new ReactiveRedisIdempotencyStore(redis.template, "test:", CLOCK);

        StepVerifier.create(store.markFailed("event-1", new IllegalStateException("boom|pipe")))
                .verifyComplete();

        verify(redis.values).set("test:event-1", "FAILED|1779062460000|boom pipe", Duration.ofMinutes(1));
    }

    @Test
    void missingKeyIsNotRecreatedWhenMarkingSuccess() {
        RedisMocks redis = redis();
        when(redis.template.getExpire("test:event-1")).thenReturn(Mono.just(Duration.ZERO));
        ReactiveRedisIdempotencyStore store = new ReactiveRedisIdempotencyStore(redis.template, "test:", CLOCK);

        StepVerifier.create(store.markSuccess("event-1"))
                .verifyComplete();

        verify(redis.values, never()).set(any(), any(), any(Duration.class));
    }

    @SuppressWarnings("unchecked")
    private static RedisMocks redis() {
        ReactiveStringRedisTemplate template = org.mockito.Mockito.mock(ReactiveStringRedisTemplate.class);
        ReactiveValueOperations<String, String> values = org.mockito.Mockito.mock(ReactiveValueOperations.class);
        when(template.opsForValue()).thenReturn(values);
        return new RedisMocks(template, values);
    }

    private record RedisMocks(
            ReactiveStringRedisTemplate template,
            ReactiveValueOperations<String, String> values
    ) {
    }
}
