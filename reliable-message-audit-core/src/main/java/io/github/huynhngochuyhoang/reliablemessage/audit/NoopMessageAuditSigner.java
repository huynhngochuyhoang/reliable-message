package io.github.huynhngochuyhoang.reliablemessage.audit;

public final class NoopMessageAuditSigner implements MessageAuditSigner {

    @Override
    public String sign(MessageAuditRecord record) {
        return null;
    }
}
