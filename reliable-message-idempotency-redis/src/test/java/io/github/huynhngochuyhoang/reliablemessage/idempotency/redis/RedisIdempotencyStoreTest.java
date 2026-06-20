package io.github.huynhngochuyhoang.reliablemessage.idempotency.redis;

import io.github.huynhngochuyhoang.reliablemessage.mvc.IdempotencyState;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class RedisIdempotencyStoreTest {

    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-05-18T00:00:00Z"), ZoneOffset.UTC);

    @Test
    void startsMissingKeyWithOptimisticTransaction() {
        StringRedisTemplate redisTemplate = org.mockito.Mockito.mock(StringRedisTemplate.class);
        TransactionStub transaction = transaction(redisTemplate, null, List.of("OK"));
        RedisIdempotencyStore store = new RedisIdempotencyStore(redisTemplate, "test:", CLOCK);

        assertTrue(store.tryStart("event-1", Duration.ofMinutes(5)).started());

        verify(transaction.operations).watch("test:event-1");
        verify(transaction.valueOperations).set("test:event-1", "PROCESSING|1779062700000|", Duration.ofMinutes(5));
        verify(transaction.operations).multi();
        verify(transaction.operations).exec();
    }

    @Test
    void existingSuccessStateIsDuplicate() {
        StringRedisTemplate redisTemplate = org.mockito.Mockito.mock(StringRedisTemplate.class);
        TransactionStub transaction = transaction(redisTemplate, "SUCCESS|1779062700000|", List.of("OK"));
        RedisIdempotencyStore store = new RedisIdempotencyStore(redisTemplate, "test:", CLOCK);

        var result = store.tryStart("event-1", Duration.ofMinutes(5));

        assertFalse(result.started());
        assertEquals(IdempotencyState.SUCCESS, result.state());
        verify(transaction.operations).unwatch();
        verify(transaction.operations, never()).multi();
    }

    @Test
    void failedStateCanStartAgain() {
        StringRedisTemplate redisTemplate = org.mockito.Mockito.mock(StringRedisTemplate.class);
        transaction(redisTemplate, "FAILED|1779062700000|boom", List.of("OK"));
        RedisIdempotencyStore store = new RedisIdempotencyStore(redisTemplate, "test:", CLOCK);

        assertTrue(store.tryStart("event-1", Duration.ofMinutes(5)).started());
    }

    @Test
    void malformedStateCanStartAgain() {
        StringRedisTemplate redisTemplate = org.mockito.Mockito.mock(StringRedisTemplate.class);
        transaction(redisTemplate, "BOGUS|1779062700000|", List.of("OK"));
        RedisIdempotencyStore store = new RedisIdempotencyStore(redisTemplate, "test:", CLOCK);

        assertTrue(store.tryStart("event-1", Duration.ofMinutes(5)).started());
    }

    @Test
    void retriesWhenOptimisticTransactionConflicts() {
        StringRedisTemplate redisTemplate = org.mockito.Mockito.mock(StringRedisTemplate.class);
        AtomicInteger executions = new AtomicInteger();
        when(redisTemplate.execute(any(SessionCallback.class))).thenAnswer(invocation -> {
            SessionCallback<?> callback = invocation.getArgument(0);
            RedisOperations<String, String> operations = org.mockito.Mockito.mock(RedisOperations.class);
            ValueOperations<String, String> valueOperations = org.mockito.Mockito.mock(ValueOperations.class);
            when(operations.opsForValue()).thenReturn(valueOperations);
            when(valueOperations.get("test:event-1")).thenReturn(null);
            when(operations.exec()).thenReturn(executions.incrementAndGet() == 1 ? null : List.of("OK"));
            return callback.execute(operations);
        });
        RedisIdempotencyStore store = new RedisIdempotencyStore(redisTemplate, "test:", CLOCK);

        assertTrue(store.tryStart("event-1", Duration.ofMinutes(5)).started());
        assertEquals(2, executions.get());
    }

    @Test
    void markSuccessPreservesRemainingTtlAndWritesSuccessState() {
        StringRedisTemplate redisTemplate = org.mockito.Mockito.mock(StringRedisTemplate.class);
        ValueOperations<String, String> valueOperations = org.mockito.Mockito.mock(ValueOperations.class);
        when(redisTemplate.getExpire("test:event-1", TimeUnit.MILLISECONDS)).thenReturn(60_000L);
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        RedisIdempotencyStore store = new RedisIdempotencyStore(redisTemplate, "test:", CLOCK);

        store.markSuccess("event-1");

        verify(valueOperations).set("test:event-1", "SUCCESS|1779062460000|", Duration.ofMinutes(1));
    }

    @Test
    void markFailedPreservesRemainingTtlAndWritesFailedState() {
        StringRedisTemplate redisTemplate = org.mockito.Mockito.mock(StringRedisTemplate.class);
        ValueOperations<String, String> valueOperations = org.mockito.Mockito.mock(ValueOperations.class);
        when(redisTemplate.getExpire("test:event-1", TimeUnit.MILLISECONDS)).thenReturn(60_000L);
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        RedisIdempotencyStore store = new RedisIdempotencyStore(redisTemplate, "test:", CLOCK);

        store.markFailed("event-1", new IllegalStateException("boom|pipe"));

        verify(valueOperations).set("test:event-1", "FAILED|1779062460000|boom pipe", Duration.ofMinutes(1));
    }

    @Test
    void missingKeyIsNotRecreatedWhenMarkingSuccess() {
        StringRedisTemplate redisTemplate = org.mockito.Mockito.mock(StringRedisTemplate.class);
        ValueOperations<String, String> valueOperations = org.mockito.Mockito.mock(ValueOperations.class);
        when(redisTemplate.getExpire("test:event-1", TimeUnit.MILLISECONDS)).thenReturn(-2L);
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        RedisIdempotencyStore store = new RedisIdempotencyStore(redisTemplate, "test:", CLOCK);

        store.markSuccess("event-1");

        verify(valueOperations, never()).set(any(), any(), any(Duration.class));
    }

    @SuppressWarnings("unchecked")
    private static TransactionStub transaction(
            StringRedisTemplate redisTemplate,
            String currentValue,
            List<Object> execResult
    ) {
        RedisOperations<String, String> operations = org.mockito.Mockito.mock(RedisOperations.class);
        ValueOperations<String, String> valueOperations = org.mockito.Mockito.mock(ValueOperations.class);
        when(operations.opsForValue()).thenReturn(valueOperations);
        when(valueOperations.get("test:event-1")).thenReturn(currentValue);
        when(operations.exec()).thenReturn(execResult);
        when(redisTemplate.execute(any(SessionCallback.class))).thenAnswer(invocation -> {
            SessionCallback<?> callback = invocation.getArgument(0);
            return callback.execute(operations);
        });
        return new TransactionStub(operations, valueOperations);
    }

    private record TransactionStub(
            RedisOperations<String, String> operations,
            ValueOperations<String, String> valueOperations
    ) {
    }
}
