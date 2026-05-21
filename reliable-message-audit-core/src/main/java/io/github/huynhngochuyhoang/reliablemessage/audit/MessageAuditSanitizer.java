package io.github.huynhngochuyhoang.reliablemessage.audit;

import java.util.Map;

public interface MessageAuditSanitizer {

    Map<String, Object> sanitizeHeaders(Map<String, Object> headers, MessageAuditContext context);

    Object sanitizePayload(Object payload, MessageAuditContext context);

    byte[] sanitizeRawBody(byte[] rawBody, MessageAuditContext context);
}
