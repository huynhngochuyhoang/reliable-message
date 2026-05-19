package io.github.huynhngochuyhoang.reliablemessage.core;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

class RetryMetadataTest {

    @Test
    void detectsExhaustedAttempts() {
        assertFalse(new RetryMetadata(2, 3, Instant.parse("2026-05-17T00:00:00Z")).exhausted());
        assertTrue(new RetryMetadata(3, 3, Instant.parse("2026-05-17T00:00:00Z")).exhausted());
    }

    @Test
    void rejectsNegativeValues() {
        assertThrows(IllegalArgumentException.class, () -> new RetryMetadata(-1, 3, null));
        assertThrows(IllegalArgumentException.class, () -> new RetryMetadata(0, -1, null));
    }
}
