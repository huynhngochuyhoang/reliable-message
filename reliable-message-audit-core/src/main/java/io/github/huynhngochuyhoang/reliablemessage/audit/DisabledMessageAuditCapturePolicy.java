package io.github.huynhngochuyhoang.reliablemessage.audit;

public final class DisabledMessageAuditCapturePolicy implements MessageAuditCapturePolicy {

    @Override
    public boolean shouldAudit(MessageAuditContext context) {
        return false;
    }

    @Override
    public boolean includeHeaders(MessageAuditContext context) {
        return false;
    }

    @Override
    public boolean includePayload(MessageAuditContext context) {
        return false;
    }

    @Override
    public boolean includeRawBody(MessageAuditContext context) {
        return false;
    }
}
