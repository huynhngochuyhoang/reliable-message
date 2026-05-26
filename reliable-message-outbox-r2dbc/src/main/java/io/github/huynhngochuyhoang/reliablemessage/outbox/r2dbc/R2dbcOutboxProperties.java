package io.github.huynhngochuyhoang.reliablemessage.outbox.r2dbc;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@ConfigurationProperties(prefix = "message.reliability.outbox")
public class R2dbcOutboxProperties {

    private boolean enabled = false;
    private boolean flushEnabled = true;
    private int batchSize = 100;
    private Duration flushDelay = Duration.ofSeconds(5);
    private Duration retryDelay = Duration.ofSeconds(30);

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

    public Duration getRetryDelay() {
        return retryDelay;
    }

    public void setRetryDelay(Duration retryDelay) {
        if (retryDelay == null || retryDelay.isNegative()) {
            throw new IllegalArgumentException("retryDelay must not be negative");
        }
        this.retryDelay = retryDelay;
    }
}
