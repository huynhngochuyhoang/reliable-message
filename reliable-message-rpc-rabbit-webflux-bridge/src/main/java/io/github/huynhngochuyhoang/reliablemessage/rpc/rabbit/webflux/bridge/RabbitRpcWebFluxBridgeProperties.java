package io.github.huynhngochuyhoang.reliablemessage.rpc.rabbit.webflux.bridge;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@ConfigurationProperties(prefix = "message.reliability.rpc.rabbit.webflux")
public class RabbitRpcWebFluxBridgeProperties {

    private boolean enabled = true;
    private Duration defaultTimeout = Duration.ofSeconds(30);
    private String exchange = "";
    private int executorThreads = Math.max(2, Runtime.getRuntime().availableProcessors());
    private int executorQueueCapacity = 1000;
    private RabbitRpcExecutorMode executorMode = RabbitRpcExecutorMode.PLATFORM;
    private int maxConcurrency = 100;
    private RpcResponseMode responseMode = RpcResponseMode.RAW;

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public Duration getDefaultTimeout() {
        return defaultTimeout;
    }

    public void setDefaultTimeout(Duration defaultTimeout) {
        if (defaultTimeout == null || defaultTimeout.isZero() || defaultTimeout.isNegative()) {
            throw new IllegalArgumentException("defaultTimeout must be positive");
        }
        this.defaultTimeout = defaultTimeout;
    }

    public String getExchange() {
        return exchange;
    }

    public void setExchange(String exchange) {
        this.exchange = exchange == null ? "" : exchange;
    }

    public int getExecutorThreads() {
        return executorThreads;
    }

    public void setExecutorThreads(int executorThreads) {
        if (executorThreads <= 0) {
            throw new IllegalArgumentException("executorThreads must be positive");
        }
        this.executorThreads = executorThreads;
    }

    public int getExecutorQueueCapacity() {
        return executorQueueCapacity;
    }

    public void setExecutorQueueCapacity(int executorQueueCapacity) {
        if (executorQueueCapacity <= 0) {
            throw new IllegalArgumentException("executorQueueCapacity must be positive");
        }
        this.executorQueueCapacity = executorQueueCapacity;
    }

    public RabbitRpcExecutorMode getExecutorMode() {
        return executorMode;
    }

    public void setExecutorMode(RabbitRpcExecutorMode executorMode) {
        if (executorMode == null) {
            throw new IllegalArgumentException("executorMode must not be null");
        }
        this.executorMode = executorMode;
    }

    public int getMaxConcurrency() {
        return maxConcurrency;
    }

    public void setMaxConcurrency(int maxConcurrency) {
        if (maxConcurrency <= 0) {
            throw new IllegalArgumentException("maxConcurrency must be positive");
        }
        this.maxConcurrency = maxConcurrency;
    }

    public RpcResponseMode getResponseMode() {
        return responseMode;
    }

    public void setResponseMode(RpcResponseMode responseMode) {
        if (responseMode == null) {
            throw new IllegalArgumentException("responseMode must not be null");
        }
        this.responseMode = responseMode;
    }
}
