package io.github.huynhngochuyhoang.reliablemessage.audit;

import reactor.core.publisher.Mono;

public interface ReactiveMessageAuditSink {

    Mono<Void> record(MessageAuditRecord record);
}
