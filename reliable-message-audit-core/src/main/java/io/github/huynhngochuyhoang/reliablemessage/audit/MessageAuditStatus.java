package io.github.huynhngochuyhoang.reliablemessage.audit;

public enum MessageAuditStatus {
    RECEIVED,
    PUBLISHED,
    CONSUMED,
    DUPLICATE,
    FAILED,
    RETRIED,
    DLQ,
    DISCARDED
}
