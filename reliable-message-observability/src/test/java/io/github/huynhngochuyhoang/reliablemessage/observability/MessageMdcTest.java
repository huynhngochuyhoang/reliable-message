package io.github.huynhngochuyhoang.reliablemessage.observability;

import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessageHeaders;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class MessageMdcTest {

    @Test
    void appliesAndRestoresMessageContext() {
        MDC.put(MessageMdc.TRACE_ID, "existing");

        try (MessageMdc.Scope ignored = MessageMdc.apply(Map.of(
                ReliableMessageHeaders.MESSAGE_ID, "message-1",
                ReliableMessageHeaders.CORRELATION_ID, "correlation-1",
                ReliableMessageHeaders.TRACE_ID, "trace-1"
        ))) {
            assertEquals("message-1", MDC.get(MessageMdc.MESSAGE_ID));
            assertEquals("correlation-1", MDC.get(MessageMdc.CORRELATION_ID));
            assertEquals("trace-1", MDC.get(MessageMdc.TRACE_ID));
        }

        assertNull(MDC.get(MessageMdc.MESSAGE_ID));
        assertNull(MDC.get(MessageMdc.CORRELATION_ID));
        assertEquals("existing", MDC.get(MessageMdc.TRACE_ID));
        MDC.clear();
    }
}
