package io.github.huynhngochuyhoang.reliablemessage.webflux;

public enum IdempotencyState {
    PROCESSING,
    SUCCESS,
    FAILED,
    EXPIRED
}
