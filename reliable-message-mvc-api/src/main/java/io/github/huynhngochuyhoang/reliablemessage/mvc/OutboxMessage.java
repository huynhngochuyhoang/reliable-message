package io.github.huynhngochuyhoang.reliablemessage.mvc;

import io.github.huynhngochuyhoang.reliablemessage.core.MessageStatus;
import io.github.huynhngochuyhoang.reliablemessage.core.PublishOptions;
import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessageHeaders;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

public record OutboxMessage(
        String id,
        String eventName,
        String aggregateId,
        String idempotencyKey,
        String partitionKey,
        Object payload,
        Map<String, String> headers,
        MessageStatus status,
        int retryCount,
        Instant nextRetryAt,
        Instant createdAt,
        Instant publishedAt,
        String lastError
) {

    public OutboxMessage {
        id = requireText(id, "id");
        eventName = requireText(eventName, "eventName");
        headers = headers == null ? Map.of() : Map.copyOf(headers);
        status = Objects.requireNonNull(status, "status must not be null");
        createdAt = Objects.requireNonNull(createdAt, "createdAt must not be null");
        if (retryCount < 0) {
            throw new IllegalArgumentException("retryCount must not be negative");
        }
    }

    public static OutboxMessage pending(String eventName, Object payload, PublishOptions options) {
        return pending(eventName, payload, options, Instant.now());
    }

    public static OutboxMessage pending(String eventName, Object payload, PublishOptions options, Instant createdAt) {
        PublishOptions safeOptions = options == null ? PublishOptions.empty() : options;
        Map<String, String> headers = new LinkedHashMap<>(safeOptions.headers());
        putIfPresent(headers, ReliableMessageHeaders.CORRELATION_ID, safeOptions.correlationId());
        return new OutboxMessage(
                UUID.randomUUID().toString(),
                eventName,
                safeOptions.aggregateId(),
                safeOptions.idempotencyKey(),
                safeOptions.partitionKey(),
                payload,
                headers,
                MessageStatus.PENDING,
                0,
                null,
                createdAt,
                null,
                null
        );
    }

    public PublishOptions toPublishOptions() {
        return PublishOptions.builder()
                .aggregateId(aggregateId)
                .idempotencyKey(idempotencyKey)
                .correlationId(headers.get(ReliableMessageHeaders.CORRELATION_ID))
                .partitionKey(partitionKey)
                .headers(headers)
                .build();
    }

    private static void putIfPresent(Map<String, String> headers, String name, String value) {
        if (value != null && !value.isBlank()) {
            headers.put(name, value);
        }
    }

    private static String requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }
}
