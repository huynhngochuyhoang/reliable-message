package io.github.huynhngochuyhoang.reliablemessage.rpc;

import java.time.Duration;
import java.util.List;

public record RpcRetryPolicy(int maxAttempts, List<Duration> backoff) {

    public RpcRetryPolicy {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be at least 1");
        }
        backoff = backoff == null ? List.of() : List.copyOf(backoff);
    }

    public static RpcRetryPolicy none() {
        return new RpcRetryPolicy(1, List.of());
    }

    public Duration delayForAttempt(int attempt) {
        if (backoff.isEmpty()) {
            return Duration.ZERO;
        }
        int index = Math.max(0, Math.min(attempt - 1, backoff.size() - 1));
        return backoff.get(index);
    }
}
