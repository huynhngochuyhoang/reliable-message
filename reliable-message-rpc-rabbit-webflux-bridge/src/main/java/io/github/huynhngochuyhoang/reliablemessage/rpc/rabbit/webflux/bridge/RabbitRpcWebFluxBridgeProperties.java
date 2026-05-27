package io.github.huynhngochuyhoang.reliablemessage.rpc.rabbit.webflux.bridge;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@ConfigurationProperties(prefix = "message.reliability.rpc.rabbit.webflux")
public class RabbitRpcWebFluxBridgeProperties {

    private boolean enabled = true;
    private Duration defaultTimeout;

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
        this.defaultTimeout = defaultTimeout;
    }
}
