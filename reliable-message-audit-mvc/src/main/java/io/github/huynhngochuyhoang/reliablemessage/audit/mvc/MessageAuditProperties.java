package io.github.huynhngochuyhoang.reliablemessage.audit.mvc;

public class MessageAuditProperties {

    private boolean enabled = false;
    private boolean includeHeaders = false;
    private boolean includePayload = false;
    private boolean includeRawBody = false;
    private boolean hashEnabled = false;
    private String onFailure = "continue-and-log";

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public boolean isIncludeHeaders() {
        return includeHeaders;
    }

    public void setIncludeHeaders(boolean includeHeaders) {
        this.includeHeaders = includeHeaders;
    }

    public boolean isIncludePayload() {
        return includePayload;
    }

    public void setIncludePayload(boolean includePayload) {
        this.includePayload = includePayload;
    }

    public boolean isIncludeRawBody() {
        return includeRawBody;
    }

    public void setIncludeRawBody(boolean includeRawBody) {
        this.includeRawBody = includeRawBody;
    }

    public boolean isHashEnabled() {
        return hashEnabled;
    }

    public void setHashEnabled(boolean hashEnabled) {
        this.hashEnabled = hashEnabled;
    }

    public String getOnFailure() {
        return onFailure;
    }

    public void setOnFailure(String onFailure) {
        this.onFailure = onFailure;
    }
}
