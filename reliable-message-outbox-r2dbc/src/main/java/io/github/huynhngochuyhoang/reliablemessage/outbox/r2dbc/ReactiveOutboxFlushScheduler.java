package io.github.huynhngochuyhoang.reliablemessage.outbox.r2dbc;

import io.github.huynhngochuyhoang.reliablemessage.webflux.OutboxMessage;
import io.github.huynhngochuyhoang.reliablemessage.webflux.ReactiveOutboxStore;
import io.github.huynhngochuyhoang.reliablemessage.webflux.ReactiveReliablePublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import reactor.core.publisher.Mono;

import java.time.Clock;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

public class ReactiveOutboxFlushScheduler {

    private static final Logger log = LoggerFactory.getLogger(ReactiveOutboxFlushScheduler.class);

    private final ReactiveOutboxStore outboxStore;
    private final ReactiveReliablePublisher reliablePublisher;
    private final R2dbcOutboxProperties properties;
    private final Clock clock;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public ReactiveOutboxFlushScheduler(
            ReactiveOutboxStore outboxStore,
            ReactiveReliablePublisher reliablePublisher,
            R2dbcOutboxProperties properties,
            Clock clock
    ) {
        this.outboxStore = Objects.requireNonNull(outboxStore, "outboxStore must not be null");
        this.reliablePublisher = Objects.requireNonNull(reliablePublisher, "reliablePublisher must not be null");
        this.properties = Objects.requireNonNull(properties, "properties must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
    }

    @Scheduled(fixedDelayString = "${message.reliability.outbox.flush-delay:5s}")
    public void flush() {
        if (!properties.isEnabled() || !properties.isFlushEnabled()) {
            return;
        }
        flushBatch().subscribe(ignored -> { }, error -> log.warn("Reliable message R2DBC outbox flush failed", error));
    }

    public Mono<Integer> flushBatch() {
        return Mono.defer(() -> {
            if (!running.compareAndSet(false, true)) {
                return Mono.just(0);
            }
            return outboxStore.findPending(properties.getBatchSize())
                    .timeout(properties.getPublishTimeout())
                    .flatMap(this::publishAndMarkSafely, properties.getBatchSize())
                    .reduce(0, Integer::sum)
                    .doFinally(ignored -> running.set(false));
        });
    }

    private Mono<Integer> publishAndMarkSafely(OutboxMessage message) {
        return publishAndMark(message)
                .onErrorResume(error -> {
                    log.warn("Reliable message R2DBC outbox item flush failed: {}", message.id(), error);
                    return Mono.just(0);
                });
    }

    private Mono<Integer> publishAndMark(OutboxMessage message) {
        return reliablePublisher.publish(message.eventName(), message.payload(), message.toPublishOptions())
                .timeout(properties.getPublishTimeout())
                .then(outboxStore.markPublished(message.id())
                        .timeout(properties.getPublishTimeout())
                        .thenReturn(1))
                .onErrorResume(error -> markFailed(message, error)
                        .timeout(properties.getPublishTimeout())
                        .thenReturn(0));
    }

    private Mono<Void> markFailed(OutboxMessage message, Throwable error) {
        Instant nextRetryAt = clock.instant().plus(properties.getRetryDelay());
        return outboxStore.markFailed(message.id(), error, nextRetryAt);
    }
}
