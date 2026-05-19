package io.github.huynhngochuyhoang.reliablemessage.core;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;

public record DeadLetterRecord(
        String id,
        String messageId,
        String eventName,
        String consumer,
        String transport,
        String payload,
        Map<String, String> headers,
        RetryMetadata retryMetadata,
        MessageError error,
        Instant deadLetteredAt
) {

    public DeadLetterRecord {
        id = requireText(id, "id");
        messageId = requireText(messageId, "messageId");
        eventName = requireText(eventName, "eventName");
        consumer = requireText(consumer, "consumer");
        transport = requireText(transport, "transport");
        headers = headers == null ? Map.of() : Map.copyOf(headers);
        Objects.requireNonNull(deadLetteredAt, "deadLetteredAt must not be null");
    }

    private static String requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }
}
