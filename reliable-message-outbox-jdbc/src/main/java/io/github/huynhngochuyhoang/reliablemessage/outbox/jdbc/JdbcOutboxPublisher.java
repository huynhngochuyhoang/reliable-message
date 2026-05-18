package io.github.huynhngochuyhoang.reliablemessage.outbox.jdbc;

import io.github.huynhngochuyhoang.reliablemessage.core.PublishOptions;
import io.github.huynhngochuyhoang.reliablemessage.mvc.OutboxMessage;
import io.github.huynhngochuyhoang.reliablemessage.mvc.OutboxPublisher;
import io.github.huynhngochuyhoang.reliablemessage.mvc.OutboxStore;
import io.github.huynhngochuyhoang.reliablemessage.observability.MessageObservability;
import io.github.huynhngochuyhoang.reliablemessage.observability.MessageTags;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.observation.ObservationRegistry;

import java.time.Clock;
import java.util.Objects;

public class JdbcOutboxPublisher implements OutboxPublisher {

    private final OutboxStore outboxStore;
    private final Clock clock;
    private final MessageObservability observability;

    public JdbcOutboxPublisher(OutboxStore outboxStore, Clock clock) {
        this(outboxStore, clock, new MessageObservability(new SimpleMeterRegistry(), ObservationRegistry.NOOP));
    }

    public JdbcOutboxPublisher(OutboxStore outboxStore, Clock clock, MessageObservability observability) {
        this.outboxStore = Objects.requireNonNull(outboxStore, "outboxStore must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
        this.observability = Objects.requireNonNull(observability, "observability must not be null");
    }

    @Override
    public void publishLater(String eventName, Object payload, PublishOptions options) {
        observability.observe(
                "message.outbox.save",
                null,
                new MessageTags("mvc", "outbox", eventName, null, "pending"),
                () -> outboxStore.save(OutboxMessage.pending(eventName, payload, options, clock.instant()))
        );
    }
}
