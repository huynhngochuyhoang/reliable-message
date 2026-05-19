package io.github.huynhngochuyhoang.reliablemessage.core;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;

public record ReliableMessage<T>(
        String messageId,
        String eventName,
        String aggregateId,
        String idempotencyKey,
        String correlationId,
        String traceId,
        Instant occurredAt,
        Map<String, String> headers,
        T payload
) {

    public ReliableMessage {
        messageId = requireText(messageId, "messageId");
        eventName = requireText(eventName, "eventName");
        occurredAt = Objects.requireNonNull(occurredAt, "occurredAt must not be null");
        headers = headers == null ? Map.of() : Map.copyOf(headers);
    }

    private static String requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }
}
