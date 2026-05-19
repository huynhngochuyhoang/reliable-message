package io.github.huynhngochuyhoang.reliablemessage.audit;

public interface MessageAuditSink {

    void record(MessageAuditRecord record);
}
