package io.github.huynhngochuyhoang.reliablemessage.outbox.jdbc;

import io.github.huynhngochuyhoang.reliablemessage.core.PublishOptions;
import io.github.huynhngochuyhoang.reliablemessage.mvc.OutboxMessage;
import io.github.huynhngochuyhoang.reliablemessage.mvc.OutboxPublisher;
import io.github.huynhngochuyhoang.reliablemessage.mvc.OutboxStore;

import java.time.Clock;
import java.util.Objects;

public class JdbcOutboxPublisher implements OutboxPublisher {

    private final OutboxStore outboxStore;
    private final Clock clock;

    public JdbcOutboxPublisher(OutboxStore outboxStore, Clock clock) {
        this.outboxStore = Objects.requireNonNull(outboxStore, "outboxStore must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
    }

    @Override
    public void publishLater(String eventName, Object payload, PublishOptions options) {
        outboxStore.save(OutboxMessage.pending(eventName, payload, options, clock.instant()));
    }
}
