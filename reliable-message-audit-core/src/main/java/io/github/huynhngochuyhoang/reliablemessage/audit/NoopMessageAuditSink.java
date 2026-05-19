package io.github.huynhngochuyhoang.reliablemessage.audit;

public final class NoopMessageAuditSink implements MessageAuditSink {

    @Override
    public void record(MessageAuditRecord record) {
        // Default is intentionally safe: audit is opt-in.
    }
}
