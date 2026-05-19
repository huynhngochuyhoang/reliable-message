package io.github.huynhngochuyhoang.reliablemessage.observability;

import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessageHeaders;
import org.slf4j.MDC;

import java.util.LinkedHashMap;
import java.util.Map;

public final class MessageMdc {

    public static final String MESSAGE_ID = "messageId";
    public static final String CORRELATION_ID = "correlationId";
    public static final String TRACE_ID = "traceId";

    private MessageMdc() {
    }

    public static Scope apply(Map<String, String> headers) {
        Map<String, String> previous = new LinkedHashMap<>();
        put(previous, MESSAGE_ID, headers == null ? null : headers.get(ReliableMessageHeaders.MESSAGE_ID));
        put(previous, CORRELATION_ID, headers == null ? null : headers.get(ReliableMessageHeaders.CORRELATION_ID));
        put(previous, TRACE_ID, headers == null ? null : headers.get(ReliableMessageHeaders.TRACE_ID));
        return new Scope(previous);
    }

    public static String currentTraceId() {
        return MDC.get(TRACE_ID);
    }

    private static void put(Map<String, String> previous, String mdcKey, String value) {
        previous.put(mdcKey, MDC.get(mdcKey));
        if (value == null || value.isBlank()) {
            MDC.remove(mdcKey);
            return;
        }
        MDC.put(mdcKey, value);
    }

    public static final class Scope implements AutoCloseable {
        private final Map<String, String> previous;

        private Scope(Map<String, String> previous) {
            this.previous = previous;
        }

        @Override
        public void close() {
            previous.forEach((key, value) -> {
                if (value == null) {
                    MDC.remove(key);
                } else {
                    MDC.put(key, value);
                }
            });
        }
    }
}
