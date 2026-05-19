package io.github.huynhngochuyhoang.reliablemessage.core;

import java.time.Instant;

public record RetryMetadata(
        int retryCount,
        int maxAttempts,
        Instant nextRetryAt
) {

    public RetryMetadata {
        if (retryCount < 0) {
            throw new IllegalArgumentException("retryCount must not be negative");
        }
        if (maxAttempts < 0) {
            throw new IllegalArgumentException("maxAttempts must not be negative");
        }
    }

    public boolean exhausted() {
        return maxAttempts > 0 && retryCount >= maxAttempts;
    }
}
