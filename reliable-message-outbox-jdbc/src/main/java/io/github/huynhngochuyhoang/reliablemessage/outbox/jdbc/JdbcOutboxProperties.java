package io.github.huynhngochuyhoang.reliablemessage.outbox.jdbc;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@ConfigurationProperties(prefix = "message.reliability.outbox")
public class JdbcOutboxProperties {

    private boolean initializeSchema = true;
    private boolean flushEnabled = true;
    private int batchSize = 100;
    private Duration flushDelay = Duration.ofSeconds(5);
    private Duration retryDelay = Duration.ofSeconds(30);

    public boolean isInitializeSchema() {
        return initializeSchema;
    }

    public void setInitializeSchema(boolean initializeSchema) {
        this.initializeSchema = initializeSchema;
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
        this.batchSize = batchSize;
    }

    public Duration getFlushDelay() {
        return flushDelay;
    }

    public void setFlushDelay(Duration flushDelay) {
        this.flushDelay = flushDelay;
    }

    public Duration getRetryDelay() {
        return retryDelay;
    }

    public void setRetryDelay(Duration retryDelay) {
        this.retryDelay = retryDelay;
    }
}
