package io.github.huynhngochuyhoang.reliablemessage.core;

import java.util.LinkedHashMap;
import java.util.Map;

public record PublishOptions(
        String aggregateId,
        String idempotencyKey,
        String correlationId,
        String partitionKey,
        Map<String, String> headers
) {

    public PublishOptions {
        headers = headers == null ? Map.of() : Map.copyOf(headers);
    }

    public static PublishOptions empty() {
        return builder().build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String aggregateId;
        private String idempotencyKey;
        private String correlationId;
        private String partitionKey;
        private final Map<String, String> headers = new LinkedHashMap<>();

        private Builder() {
        }

        public Builder aggregateId(String aggregateId) {
            this.aggregateId = aggregateId;
            return this;
        }

        public Builder idempotencyKey(String idempotencyKey) {
            this.idempotencyKey = idempotencyKey;
            return this;
        }

        public Builder correlationId(String correlationId) {
            this.correlationId = correlationId;
            return this;
        }

        public Builder partitionKey(String partitionKey) {
            this.partitionKey = partitionKey;
            return this;
        }

        public Builder header(String name, String value) {
            headers.put(name, value);
            return this;
        }

        public Builder headers(Map<String, String> headers) {
            if (headers != null) {
                this.headers.putAll(headers);
            }
            return this;
        }

        public PublishOptions build() {
            return new PublishOptions(aggregateId, idempotencyKey, correlationId, partitionKey, headers);
        }
    }
}
