package io.github.huynhngochuyhoang.reliablemessage.audit;

import reactor.core.publisher.Mono;

public final class NoopReactiveMessageAuditSink implements ReactiveMessageAuditSink {

    @Override
    public Mono<Void> record(MessageAuditRecord record) {
        return Mono.empty();
    }
}
