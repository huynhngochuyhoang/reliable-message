package io.github.huynhngochuyhoang.reliablemessage.outbox.r2dbc;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@ConfigurationProperties(prefix = "message.reliability.outbox")
public class R2dbcOutboxProperties {

    public enum PayloadStorage {
        TEXT,
        JSON,
        BINARY
    }

    private boolean enabled = false;
    private boolean flushEnabled = true;
    private int batchSize = 100;
    private Duration flushDelay = Duration.ofSeconds(5);
    private Duration retryDelay = Duration.ofSeconds(30);
    private Duration publishTimeout = Duration.ofSeconds(30);
    private final Schema schema = new Schema();

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public boolean isFlushEnabled() {
        return flushEnabled;
    }

    public void setFlushEnabled(boolean flushEnabled) {
        this.flushEnabled = flushEnabled;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be positive");
        }
        this.batchSize = batchSize;
    }

    public Duration getFlushDelay() {
        return flushDelay;
    }

    public void setFlushDelay(Duration flushDelay) {
        if (flushDelay == null || flushDelay.isNegative() || flushDelay.isZero()) {
            throw new IllegalArgumentException("flushDelay must be positive");
        }
        this.flushDelay = flushDelay;
    }

    public Duration getPublishTimeout() {
        return publishTimeout;
    }

    public void setPublishTimeout(Duration publishTimeout) {
        if (publishTimeout == null || publishTimeout.isNegative() || publishTimeout.isZero()) {
            throw new IllegalArgumentException("publishTimeout must be positive");
        }
        this.publishTimeout = publishTimeout;
    }

    public Duration getRetryDelay() {
        return retryDelay;
    }

    public void setRetryDelay(Duration retryDelay) {
        if (retryDelay == null || retryDelay.isNegative()) {
            throw new IllegalArgumentException("retryDelay must not be negative");
        }
        this.retryDelay = retryDelay;
    }

    public Schema getSchema() {
        return schema;
    }

    public static class Schema {
        private PayloadStorage payloadStorage = PayloadStorage.TEXT;
        private String payloadColumnType;
        private String headersColumnType;
        private String payloadBytesColumnType;
        private String lastErrorColumnType;

        public PayloadStorage getPayloadStorage() {
            return payloadStorage;
        }

        public void setPayloadStorage(PayloadStorage payloadStorage) {
            if (payloadStorage == null) {
                throw new IllegalArgumentException("payloadStorage must not be null");
            }
            this.payloadStorage = payloadStorage;
        }

        public String getPayloadColumnType() {
            return payloadColumnType;
        }

        public void setPayloadColumnType(String payloadColumnType) {
            this.payloadColumnType = columnType(payloadColumnType, "payloadColumnType");
        }

        public String getHeadersColumnType() {
            return headersColumnType;
        }

        public void setHeadersColumnType(String headersColumnType) {
            this.headersColumnType = columnType(headersColumnType, "headersColumnType");
        }

        public String getPayloadBytesColumnType() {
            return payloadBytesColumnType;
        }

        public void setPayloadBytesColumnType(String payloadBytesColumnType) {
            this.payloadBytesColumnType = columnType(payloadBytesColumnType, "payloadBytesColumnType");
        }

        public String getLastErrorColumnType() {
            return lastErrorColumnType;
        }

        public void setLastErrorColumnType(String lastErrorColumnType) {
            this.lastErrorColumnType = columnType(lastErrorColumnType, "lastErrorColumnType");
        }

        private static String columnType(String value, String name) {
            if (value != null && value.isBlank()) {
                throw new IllegalArgumentException(name + " must not be blank");
            }
            return value;
        }
    }
}
