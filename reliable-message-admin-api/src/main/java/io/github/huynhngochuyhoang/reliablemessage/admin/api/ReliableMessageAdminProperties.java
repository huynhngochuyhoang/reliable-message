package io.github.huynhngochuyhoang.reliablemessage.admin.api;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "message.reliability.admin")
public class ReliableMessageAdminProperties {

    private boolean enabled = false;
    private int defaultLimit = 50;
    private int maxLimit = 200;

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public int getDefaultLimit() {
        return defaultLimit;
    }

    public void setDefaultLimit(int defaultLimit) {
        this.defaultLimit = defaultLimit;
    }

    public int getMaxLimit() {
        return maxLimit;
    }

    public void setMaxLimit(int maxLimit) {
        this.maxLimit = maxLimit;
    }

    public int clampLimit(Integer requestedLimit) {
        int effectiveDefault = defaultLimit <= 0 ? 50 : defaultLimit;
        int effectiveMax = maxLimit <= 0 ? 200 : maxLimit;
        int limit = requestedLimit == null ? effectiveDefault : requestedLimit;
        if (limit <= 0) {
            return effectiveDefault;
        }
        return Math.min(limit, effectiveMax);
    }
}
