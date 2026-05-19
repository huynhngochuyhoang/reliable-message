package io.github.huynhngochuyhoang.reliablemessage.outbox.jdbc;

import io.github.huynhngochuyhoang.reliablemessage.mvc.OutboxMessage;
import io.github.huynhngochuyhoang.reliablemessage.mvc.OutboxStore;
import io.github.huynhngochuyhoang.reliablemessage.mvc.ReliablePublisher;
import io.github.huynhngochuyhoang.reliablemessage.observability.MessageObservability;
import io.github.huynhngochuyhoang.reliablemessage.observability.MessageTags;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.observation.ObservationRegistry;
import org.springframework.scheduling.annotation.Scheduled;

import java.time.Clock;
import java.util.Objects;

public class OutboxFlushScheduler {

    private final OutboxStore outboxStore;
    private final ReliablePublisher reliablePublisher;
    private final JdbcOutboxProperties properties;
    private final Clock clock;
    private final MessageObservability observability;

    public OutboxFlushScheduler(
            OutboxStore outboxStore,
            ReliablePublisher reliablePublisher,
            JdbcOutboxProperties properties,
            Clock clock
    ) {
        this(outboxStore, reliablePublisher, properties, clock, new MessageObservability(new SimpleMeterRegistry(), ObservationRegistry.NOOP));
    }

    public OutboxFlushScheduler(
            OutboxStore outboxStore,
            ReliablePublisher reliablePublisher,
            JdbcOutboxProperties properties,
            Clock clock,
            MessageObservability observability
    ) {
        this.outboxStore = Objects.requireNonNull(outboxStore, "outboxStore must not be null");
        this.reliablePublisher = Objects.requireNonNull(reliablePublisher, "reliablePublisher must not be null");
        this.properties = Objects.requireNonNull(properties, "properties must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
        this.observability = Objects.requireNonNull(observability, "observability must not be null");
    }

    @Scheduled(fixedDelayString = "${message.reliability.outbox.flush-delay:5s}")
    public void flush() {
        if (properties.isFlushEnabled()) {
            flushBatch();
        }
    }

    public int flushBatch() {
        return observability.observe(
                "message.outbox.flush",
                "message_outbox_publish_duration",
                new MessageTags("mvc", "outbox", "batch", null, "flush"),
                () -> {
                    int published = 0;
                    for (OutboxMessage message : outboxStore.findPending(properties.getBatchSize())) {
                        observability.increment("message_outbox_pending_total",
                                new MessageTags("mvc", "outbox", message.eventName(), null, message.status().name().toLowerCase()));
                        try {
                            reliablePublisher.publish(message.eventName(), message.payload(), message.toPublishOptions());
                            outboxStore.markPublished(message.id());
                            published++;
                        } catch (RuntimeException error) {
                            outboxStore.markFailed(message.id(), error, clock.instant().plus(properties.getRetryDelay()));
                        }
                    }
                    return published;
                }
        );
    }
}
