package io.github.huynhngochuyhoang.reliablemessage.mvc;

public enum IdempotencyState {
    PROCESSING,
    SUCCESS,
    FAILED,
    EXPIRED
}
