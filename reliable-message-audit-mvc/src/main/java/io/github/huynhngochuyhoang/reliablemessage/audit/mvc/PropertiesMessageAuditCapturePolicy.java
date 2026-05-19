package io.github.huynhngochuyhoang.reliablemessage.audit.mvc;

import io.github.huynhngochuyhoang.reliablemessage.audit.MessageAuditCapturePolicy;
import io.github.huynhngochuyhoang.reliablemessage.audit.MessageAuditContext;

public class PropertiesMessageAuditCapturePolicy implements MessageAuditCapturePolicy {

    private final MessageAuditProperties properties;

    public PropertiesMessageAuditCapturePolicy(MessageAuditProperties properties) {
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
