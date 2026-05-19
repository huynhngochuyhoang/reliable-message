package io.github.huynhngochuyhoang.reliablemessage.admin.api;

import io.github.huynhngochuyhoang.reliablemessage.mvc.IdempotencyState;

import java.time.Instant;

public record IdempotencyRecord(
        String key,
        IdempotencyState state,
        Instant expiresAt,
        String lastError
) {
}
