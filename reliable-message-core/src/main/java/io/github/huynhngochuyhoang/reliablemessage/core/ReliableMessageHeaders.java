package io.github.huynhngochuyhoang.reliablemessage.core;

public final class ReliableMessageHeaders {

    public static final String MESSAGE_ID = "x-message-id";
    public static final String EVENT_NAME = "x-event-name";
    public static final String AGGREGATE_ID = "x-aggregate-id";
    public static final String IDEMPOTENCY_KEY = "x-idempotency-key";
    public static final String CORRELATION_ID = "x-correlation-id";
    public static final String TRACE_ID = "x-trace-id";
    public static final String OCCURRED_AT = "x-occurred-at";
    public static final String PARTITION_KEY = "x-partition-key";
    public static final String RETRY_COUNT = "x-retry-count";
    public static final String ORIGINAL_MESSAGE_ID = "x-original-message-id";

    private ReliableMessageHeaders() {
    }
}
