package io.github.huynhngochuyhoang.reliablemessage.core;

import java.time.Instant;
import java.util.Objects;

public record MessageError(
        String errorType,
        String message,
        Instant occurredAt
) {

    public MessageError {
        errorType = errorType == null || errorType.isBlank() ? "unknown" : errorType;
        occurredAt = Objects.requireNonNull(occurredAt, "occurredAt must not be null");
    }

    public static MessageError from(Throwable error, Instant occurredAt) {
        Objects.requireNonNull(error, "error must not be null");
        return new MessageError(error.getClass().getName(), error.getMessage(), occurredAt);
    }
}
