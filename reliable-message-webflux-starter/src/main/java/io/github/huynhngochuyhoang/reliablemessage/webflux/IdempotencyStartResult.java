package io.github.huynhngochuyhoang.reliablemessage.webflux;

import java.util.Objects;

public record IdempotencyStartResult(
        boolean started,
        IdempotencyState state
) {

    public IdempotencyStartResult {
        state = Objects.requireNonNull(state, "state must not be null");
    }

    public static IdempotencyStartResult startAccepted() {
        return new IdempotencyStartResult(true, IdempotencyState.PROCESSING);
    }

    public static IdempotencyStartResult duplicate(IdempotencyState state) {
        return new IdempotencyStartResult(false, state);
    }
}
