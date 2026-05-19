package io.github.huynhngochuyhoang.reliablemessage.audit.webflux;

import io.github.huynhngochuyhoang.reliablemessage.audit.MessageAuditCapturePolicy;
import io.github.huynhngochuyhoang.reliablemessage.audit.MessageAuditContext;

public class ReactivePropertiesMessageAuditCapturePolicy implements MessageAuditCapturePolicy {

    private final ReactiveMessageAuditProperties properties;

    public ReactivePropertiesMessageAuditCapturePolicy(ReactiveMessageAuditProperties properties) {
        this.properties = properties;
    }

    @Override
    public boolean shouldAudit(MessageAuditContext context) {
        return properties.isEnabled();
    }

    @Override
    public boolean includeHeaders(MessageAuditContext context) {
        return properties.isIncludeHeaders();
    }

    @Override
    public boolean includePayload(MessageAuditContext context) {
        return properties.isIncludePayload();
    }

    @Override
    public boolean includeRawBody(MessageAuditContext context) {
        return properties.isIncludeRawBody();
    }
}
