package io.github.huynhngochuyhoang.reliablemessage.audit;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

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

    @Test
    void hasherSkipsInaccessibleFields() {
        Sha256MessageAuditHasher hasher = new Sha256MessageAuditHasher();
        Object payload = Map.of("createdAt", Instant.parse("2026-05-18T00:00:00Z"));

        assertThatCode(() -> hasher.hashPayload(payload)).doesNotThrowAnyException();
    }

    @Test
    void hasherUsesStableArrayValues() {
        Sha256MessageAuditHasher hasher = new Sha256MessageAuditHasher();

        String first = hasher.hashPayload(new String[]{"a", "b"});
        String second = hasher.hashPayload(new String[]{"a", "b"});

        assertThat(first).isEqualTo(second);
    }

    @Test
    void hasherBreaksPayloadCycles() {
        Sha256MessageAuditHasher hasher = new Sha256MessageAuditHasher();
        Node parent = new Node("parent");
        Node child = new Node("child");
        parent.next = child;
        child.next = parent;

        assertThatCode(() -> hasher.hashPayload(parent)).doesNotThrowAnyException();
    }

    private static final class Node {
        private final String name;
        private Node next;

        private Node(String name) {
            this.name = name;
        }
    }
}
