package io.github.huynhngochuyhoang.reliablemessage.core;

public enum MessageStatus {
    PENDING,
    PROCESSING,
    PUBLISHED,
    SUCCESS,
    FAILED,
    DEAD_LETTERED,
    DISCARDED,
    EXPIRED
}
