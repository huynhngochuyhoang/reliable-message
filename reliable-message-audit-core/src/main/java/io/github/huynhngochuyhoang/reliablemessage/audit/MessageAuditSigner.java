package io.github.huynhngochuyhoang.reliablemessage.audit;

public interface MessageAuditSigner {

    String sign(MessageAuditRecord record);
}
