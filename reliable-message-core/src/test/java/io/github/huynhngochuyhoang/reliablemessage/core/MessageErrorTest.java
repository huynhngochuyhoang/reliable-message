package io.github.huynhngochuyhoang.reliablemessage.core;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MessageErrorTest {

    @Test
    void createsErrorFromThrowable() {
        IllegalStateException error = new IllegalStateException("boom");
        MessageError messageError = MessageError.from(error, Instant.parse("2026-05-17T00:00:00Z"));

        assertEquals(IllegalStateException.class.getName(), messageError.errorType());
        assertEquals("boom", messageError.message());
    }
}
