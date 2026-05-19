package io.github.huynhngochuyhoang.reliablemessage.audit;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class MessageAuditSafetyTest {

    @Test
    void defaultCapturePolicyDisablesAuditAndFullCapture() {
        DisabledMessageAuditCapturePolicy policy = new DisabledMessageAuditCapturePolicy();
        MessageAuditContext context = new MessageAuditContext(
                MessageDirection.PUBLISH,
                "mvc",
                "rabbit",
                "orders",
                "order.created",
                "orders.exchange",
                Map.of()
        );

        assertThat(policy.shouldAudit(context)).isFalse();
        assertThat(policy.includeHeaders(context)).isFalse();
        assertThat(policy.includePayload(context)).isFalse();
        assertThat(policy.includeRawBody(context)).isFalse();
    }

    @Test
    void sanitizerMasksSensitiveHeaders() {
        DefaultMessageAuditSanitizer sanitizer = new DefaultMessageAuditSanitizer();
        Map<String, Object> sanitized = sanitizer.sanitizeHeaders(
                Map.of("authorization", "Bearer secret", "x-correlation-id", "correlation-1"),
                new MessageAuditContext(MessageDirection.CONSUME, "mvc", "kafka", "orders", "order.created", "topic", Map.of())
        );

        assertThat(sanitized)
                .containsEntry("authorization", "[REDACTED]")
                .containsEntry("x-correlation-id", "correlation-1");
    }
}
