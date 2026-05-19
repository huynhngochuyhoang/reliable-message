package io.github.huynhngochuyhoang.reliablemessage.audit.webflux;

import io.github.huynhngochuyhoang.reliablemessage.audit.MessageAuditCapturePolicy;
import io.github.huynhngochuyhoang.reliablemessage.audit.MessageAuditContext;
import io.github.huynhngochuyhoang.reliablemessage.audit.MessageAuditHasher;
import io.github.huynhngochuyhoang.reliablemessage.audit.MessageAuditRecord;
import io.github.huynhngochuyhoang.reliablemessage.audit.MessageAuditSanitizer;
import io.github.huynhngochuyhoang.reliablemessage.audit.MessageAuditSigner;
import io.github.huynhngochuyhoang.reliablemessage.audit.ReactiveMessageAuditSink;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.Objects;

public class ReactiveMessageAuditRecorder {

    private final MessageAuditCapturePolicy capturePolicy;
    private final MessageAuditSanitizer sanitizer;
    private final MessageAuditHasher hasher;
    private final MessageAuditSigner signer;
    private final ReactiveMessageAuditSink sink;
    private final MeterRegistry meterRegistry;
    private final ReactiveMessageAuditProperties properties;

    public ReactiveMessageAuditRecorder(
            MessageAuditCapturePolicy capturePolicy,
            MessageAuditSanitizer sanitizer,
            MessageAuditHasher hasher,
            MessageAuditSigner signer,
            ReactiveMessageAuditSink sink,
            MeterRegistry meterRegistry,
            ReactiveMessageAuditProperties properties
    ) {
        this.capturePolicy = Objects.requireNonNull(capturePolicy, "capturePolicy must not be null");
        this.sanitizer = Objects.requireNonNull(sanitizer, "sanitizer must not be null");
        this.hasher = Objects.requireNonNull(hasher, "hasher must not be null");
        this.signer = Objects.requireNonNull(signer, "signer must not be null");
        this.sink = Objects.requireNonNull(sink, "sink must not be null");
        this.meterRegistry = Objects.requireNonNull(meterRegistry, "meterRegistry must not be null");
        this.properties = Objects.requireNonNull(properties, "properties must not be null");
    }

    public Mono<Void> record(MessageAuditRecord record) {
        return Mono.defer(() -> {
            MessageAuditContext context = context(record);
            if (!capturePolicy.shouldAudit(context)) {
                return Mono.empty();
            }

            Timer.Sample sample = Timer.start(meterRegistry);
            MessageAuditRecord sanitized = sanitize(record, context);
            String signature = signer.sign(sanitized);
            MessageAuditRecord finalRecord = sanitized.withCapture(
                    sanitized.headers(),
                    sanitized.payload(),
                    sanitized.rawBody(),
                    sanitized.payloadHash(),
                    sanitized.headersHash(),
                    signature
            );
            return sink.record(finalRecord)
                    .doOnSuccess(ignored -> {
                        increment("message_audit_reactive_records_total", "recorded");
                        sample.stop(timer("recorded"));
                    })
                    .doOnError(error -> {
                        increment("message_audit_reactive_failed_total", error.getClass().getSimpleName());
                        sample.stop(timer("failed"));
                    })
                    .onErrorResume(error -> "fail-business".equals(properties.getOnFailure()) ? Mono.error(error) : Mono.empty());
        });
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
        return Timer.builder("message_audit_reactive_duration")
                .tag("status", status)
                .register(meterRegistry);
    }
}
