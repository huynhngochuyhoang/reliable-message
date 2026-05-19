package io.github.huynhngochuyhoang.reliablemessage.core;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ReliableMessageTest {

    @Test
    void copiesHeadersDefensively() {
        Map<String, String> headers = new HashMap<>();
        headers.put(ReliableMessageHeaders.CORRELATION_ID, "correlation-1");

        ReliableMessage<String> message = new ReliableMessage<>(
                "message-1",
                "order.created",
                "order-1",
                "event-1",
                "correlation-1",
                "trace-1",
                Instant.parse("2026-05-17T00:00:00Z"),
                headers,
                "payload"
        );

        headers.put(ReliableMessageHeaders.CORRELATION_ID, "changed");

        assertEquals("correlation-1", message.headers().get(ReliableMessageHeaders.CORRELATION_ID));
    }

    @Test
    void requiresMessageIdAndEventName() {
        assertThrows(IllegalArgumentException.class, () -> new ReliableMessage<>(
                "",
                "order.created",
                null,
                null,
                null,
                null,
                Instant.parse("2026-05-17T00:00:00Z"),
                Map.of(),
                "payload"
        ));

        assertThrows(IllegalArgumentException.class, () -> new ReliableMessage<>(
                "message-1",
                " ",
                null,
                null,
                null,
                null,
                Instant.parse("2026-05-17T00:00:00Z"),
                Map.of(),
                "payload"
        ));
    }
}
