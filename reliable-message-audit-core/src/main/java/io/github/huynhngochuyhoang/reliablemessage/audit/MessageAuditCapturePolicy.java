package io.github.huynhngochuyhoang.reliablemessage.audit;

public interface MessageAuditCapturePolicy {

    boolean shouldAudit(MessageAuditContext context);

    boolean includeHeaders(MessageAuditContext context);

    boolean includePayload(MessageAuditContext context);

    boolean includeRawBody(MessageAuditContext context);
}
