package io.github.huynhngochuyhoang.reliablemessage.audit;

import java.time.Instant;
import java.util.Map;

public record MessageAuditRecord(
        String auditId,
        MessageDirection direction,
        String runtime,
        String transport,
        String serviceName,
        String eventName,
        String messageId,
        String correlationId,
        String traceId,
        String aggregateId,
        String idempotencyKey,
        String destination,
        Map<String, Object> headers,
        Object payload,
        byte[] rawBody,
        Instant occurredAt,
        MessageAuditStatus status,
        Integer attempt,
        Long durationMs,
        String errorClass,
        String errorMessage,
        String payloadHash,
        String headersHash,
        String signature
) {

    public MessageAuditRecord {
        headers = headers == null ? Map.of() : Map.copyOf(headers);
        rawBody = rawBody == null ? null : rawBody.clone();
        occurredAt = occurredAt == null ? Instant.now() : occurredAt;
    }

    @Override
    public byte[] rawBody() {
        return rawBody == null ? null : rawBody.clone();
    }

    public MessageAuditRecord withCapture(Map<String, Object> headers, Object payload, byte[] rawBody, String payloadHash, String headersHash, String signature) {
        return new MessageAuditRecord(
                auditId,
                direction,
                runtime,
                transport,
                serviceName,
                eventName,
                messageId,
                correlationId,
                traceId,
                aggregateId,
                idempotencyKey,
                destination,
                headers,
                payload,
                rawBody,
                occurredAt,
                status,
                attempt,
                durationMs,
                errorClass,
                errorMessage,
                payloadHash,
                headersHash,
                signature
        );
    }
}
