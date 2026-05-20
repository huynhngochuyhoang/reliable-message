package io.github.huynhngochuyhoang.reliablemessage.rpc;

import java.util.LinkedHashMap;
import java.util.Map;

public final class RpcHeaders {

    public static final String CORRELATION_ID = "x-correlation-id";
    public static final String REQUEST_ID = "x-request-id";
    public static final String TRACE_ID = "x-trace-id";
    public static final String TENANT_ID = "x-tenant-id";

    private RpcHeaders() {
    }

    public static Map<String, String> from(RpcContext context) {
        if (context == null) {
            return Map.of();
        }
        Map<String, String> headers = new LinkedHashMap<>();
        put(headers, CORRELATION_ID, context.correlationId());
        put(headers, REQUEST_ID, context.requestId());
        put(headers, TRACE_ID, context.traceId());
        put(headers, TENANT_ID, context.tenantId());
        context.headers().forEach((name, value) -> headers.putIfAbsent(name, value));
        return Map.copyOf(headers);
    }

    private static void put(Map<String, String> headers, String name, String value) {
        if (value != null && !value.isBlank()) {
            headers.put(name, value);
        }
    }
}
