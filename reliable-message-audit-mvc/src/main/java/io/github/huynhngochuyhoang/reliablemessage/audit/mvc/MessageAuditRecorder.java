package io.github.huynhngochuyhoang.reliablemessage.audit.mvc;

import io.github.huynhngochuyhoang.reliablemessage.audit.MessageAuditCapturePolicy;
import io.github.huynhngochuyhoang.reliablemessage.audit.MessageAuditContext;
import io.github.huynhngochuyhoang.reliablemessage.audit.MessageAuditHasher;
import io.github.huynhngochuyhoang.reliablemessage.audit.MessageAuditRecord;
import io.github.huynhngochuyhoang.reliablemessage.audit.MessageAuditSanitizer;
import io.github.huynhngochuyhoang.reliablemessage.audit.MessageAuditSigner;
import io.github.huynhngochuyhoang.reliablemessage.audit.MessageAuditSink;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import java.util.Map;
import java.util.Objects;

public class MessageAuditRecorder {

    private final MessageAuditCapturePolicy capturePolicy;
    private final MessageAuditSanitizer sanitizer;
    private final MessageAuditHasher hasher;
    private final MessageAuditSigner signer;
    private final MessageAuditSink sink;
    private final MeterRegistry meterRegistry;
    private final MessageAuditProperties properties;

    public MessageAuditRecorder(
            MessageAuditCapturePolicy capturePolicy,
            MessageAuditSanitizer sanitizer,
            MessageAuditHasher hasher,
            MessageAuditSigner signer,
            MessageAuditSink sink,
            MeterRegistry meterRegistry,
            MessageAuditProperties properties
    ) {
        this.capturePolicy = Objects.requireNonNull(capturePolicy, "capturePolicy must not be null");
        this.sanitizer = Objects.requireNonNull(sanitizer, "sanitizer must not be null");
        this.hasher = Objects.requireNonNull(hasher, "hasher must not be null");
        this.signer = Objects.requireNonNull(signer, "signer must not be null");
        this.sink = Objects.requireNonNull(sink, "sink must not be null");
        this.meterRegistry = Objects.requireNonNull(meterRegistry, "meterRegistry must not be null");
        this.properties = Objects.requireNonNull(properties, "properties must not be null");
    }

    public void record(MessageAuditRecord record) {
        MessageAuditContext context = context(record);
        if (!capturePolicy.shouldAudit(context)) {
            return;
        }

        Timer.Sample sample = Timer.start(meterRegistry);
        try {
            MessageAuditRecord sanitized = sanitize(record, context);
            String signature = signer.sign(sanitized);
            sink.record(sanitized.withCapture(
                    sanitized.headers(),
                    sanitized.payload(),
                    sanitized.rawBody(),
                    sanitized.payloadHash(),
                    sanitized.headersHash(),
                    signature
            ));
            increment("message_audit_records_total", "recorded");
            sample.stop(timer("recorded"));
        } catch (RuntimeException error) {
            increment("message_audit_failed_total", error.getClass().getSimpleName());
            sample.stop(timer("failed"));
            if ("fail-business".equals(properties.getOnFailure())) {
                throw error;
            }
        }
    }

    private MessageAuditRecord sanitize(MessageAuditRecord record, MessageAuditContext context) {
        Map<String, Object> headers = capturePolicy.includeHeaders(context)
                ? sanitizer.sanitizeHeaders(record.headers(), context)
                : Map.of();
        Object payload = capturePolicy.includePayload(context)
                ? sanitizer.sanitizePayload(record.payload(), context)
                : null;
        byte[] rawBody = capturePolicy.includeRawBody(context)
                ? sanitizer.sanitizeRawBody(record.rawBody(), context)
                : null;

        String payloadHash = properties.isHashEnabled() ? hasher.hashPayload(payload) : null;
        String headersHash = properties.isHashEnabled() ? hasher.hashHeaders(headers) : null;
        if (payloadHash == null && rawBody != null && properties.isHashEnabled()) {
            payloadHash = hasher.hashRawBody(rawBody);
        }
        return record.withCapture(headers, payload, rawBody, payloadHash, headersHash, null);
    }

    private static MessageAuditContext context(MessageAuditRecord record) {
        return new MessageAuditContext(
                record.direction(),
                record.runtime(),
                record.transport(),
                record.serviceName(),
                record.eventName(),
                record.destination(),
                record.headers()
        );
    }

    private void increment(String metricName, String status) {
        Counter.builder(metricName)
                .tag("status", status)
                .register(meterRegistry)
                .increment();
    }

    private Timer timer(String status) {
        return Timer.builder("message_audit_duration")
                .tag("status", status)
                .register(meterRegistry);
    }
}
