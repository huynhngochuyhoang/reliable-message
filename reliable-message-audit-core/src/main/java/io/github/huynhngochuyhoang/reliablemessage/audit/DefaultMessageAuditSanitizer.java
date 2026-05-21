package io.github.huynhngochuyhoang.reliablemessage.audit;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public final class DefaultMessageAuditSanitizer implements MessageAuditSanitizer {

    private static final Set<String> SENSITIVE_HEADERS = Set.of(
            "authorization",
            "cookie",
            "set-cookie",
            "x-api-key",
            "api-key",
            "token",
            "secret",
            "password"
    );

    @Override
    public Map<String, Object> sanitizeHeaders(Map<String, Object> headers, MessageAuditContext context) {
        if (headers == null || headers.isEmpty()) {
            return Map.of();
        }
        Map<String, Object> sanitized = new LinkedHashMap<>();
        headers.forEach((name, value) -> {
            String lowerName = name == null ? "" : name.toLowerCase(Locale.ROOT);
            sanitized.put(name, SENSITIVE_HEADERS.contains(lowerName) ? "[REDACTED]" : value);
        });
        return Map.copyOf(sanitized);
    }

    @Override
    public Object sanitizePayload(Object payload, MessageAuditContext context) {
        return payload;
    }

    @Override
    public byte[] sanitizeRawBody(byte[] rawBody, MessageAuditContext context) {
        return rawBody == null ? null : rawBody.clone();
    }
}
