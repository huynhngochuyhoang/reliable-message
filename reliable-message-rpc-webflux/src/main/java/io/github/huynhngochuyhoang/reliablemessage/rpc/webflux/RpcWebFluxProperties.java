package io.github.huynhngochuyhoang.reliablemessage.rpc.webflux;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

@ConfigurationProperties(prefix = "message.reliability.rpc.webflux")
public class RpcWebFluxProperties {

    private boolean enabled = true;
    private int maxAttempts = 1;
    private Duration requestTimeout;
    private List<Duration> backoff = new ArrayList<>();

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public int getMaxAttempts() {
        return maxAttempts;
    }

    public void setMaxAttempts(int maxAttempts) {
        this.maxAttempts = maxAttempts;
    }

    public Duration getRequestTimeout() {
        return requestTimeout;
    }

    public void setRequestTimeout(Duration requestTimeout) {
        this.requestTimeout = requestTimeout;
    }

    public List<Duration> getBackoff() {
        return backoff;
    }

    public void setBackoff(List<Duration> backoff) {
        this.backoff = backoff == null ? new ArrayList<>() : new ArrayList<>(backoff);
    }
}
