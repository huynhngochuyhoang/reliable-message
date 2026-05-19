package io.github.huynhngochuyhoang.reliablemessage.audit;

import java.util.Map;

public record MessageAuditContext(
        MessageDirection direction,
        String runtime,
        String transport,
        String serviceName,
        String eventName,
        String destination,
        Map<String, Object> headers
) {

    public MessageAuditContext {
        headers = headers == null ? Map.of() : Map.copyOf(headers);
    }
}
