package io.github.huynhngochuyhoang.reliablemessage.rpc;

import java.util.LinkedHashMap;
import java.util.Map;

public record RpcContext(
        String correlationId,
        String requestId,
        String traceId,
        String tenantId,
        Map<String, String> headers
) {

    public RpcContext {
        headers = headers == null ? Map.of() : Map.copyOf(headers);
    }

    public static RpcContext empty() {
        return new RpcContext(null, null, null, null, Map.of());
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String correlationId;
        private String requestId;
        private String traceId;
        private String tenantId;
        private final Map<String, String> headers = new LinkedHashMap<>();

        private Builder() {
        }

        public Builder correlationId(String correlationId) {
            this.correlationId = correlationId;
            return this;
        }

        public Builder requestId(String requestId) {
            this.requestId = requestId;
            return this;
        }

        public Builder traceId(String traceId) {
            this.traceId = traceId;
            return this;
        }

        public Builder tenantId(String tenantId) {
            this.tenantId = tenantId;
            return this;
        }

        public Builder header(String name, String value) {
            if (name != null && !name.isBlank() && value != null && !value.isBlank()) {
                headers.put(name, value);
            }
            return this;
        }

        public RpcContext build() {
            return new RpcContext(correlationId, requestId, traceId, tenantId, headers);
        }
    }
}
