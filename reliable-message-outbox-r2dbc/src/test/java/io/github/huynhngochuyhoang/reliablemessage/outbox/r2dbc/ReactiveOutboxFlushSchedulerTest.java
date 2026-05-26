package io.github.huynhngochuyhoang.reliablemessage.outbox.r2dbc;

import io.github.huynhngochuyhoang.reliablemessage.core.MessageStatus;
import io.github.huynhngochuyhoang.reliablemessage.core.PublishOptions;
import io.github.huynhngochuyhoang.reliablemessage.webflux.OutboxMessage;
import io.github.huynhngochuyhoang.reliablemessage.webflux.ReactiveOutboxStore;
import io.github.huynhngochuyhoang.reliablemessage.webflux.ReactiveReliablePublisher;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.test.publisher.PublisherProbe;
import reactor.test.publisher.TestPublisher;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ReactiveOutboxFlushSchedulerTest {

    private static final Instant NOW = Instant.parse("2026-05-18T00:00:00Z");

    @Test
    void readsPendingRecordsWithConfiguredBatchSize() {
        RecordingOutboxStore store = new RecordingOutboxStore(List.of(message("event-1")));
        RecordingPublisher publisher = new RecordingPublisher();
        R2dbcOutboxProperties properties = properties();
        properties.setBatchSize(25);

        StepVerifier.create(scheduler(store, publisher, properties).flushBatch())
                .expectNext(1)
                .verifyComplete();

        assertThat(store.lastLimit()).isEqualTo(25);
    }

    @Test
    void publishesPendingRecordAndMarksPublishedAfterSuccess() {
        RecordingOutboxStore store = new RecordingOutboxStore(List.of(message("event-1")));
        RecordingPublisher publisher = new RecordingPublisher();

        StepVerifier.create(scheduler(store, publisher, properties()).flushBatch())
                .expectNext(1)
                .verifyComplete();

        assertThat(publisher.publishedEventNames()).containsExactly("order.created");
        assertThat(store.events()).containsExactly("findPending", "markPublished:event-1");
    }

    @Test
    void marksFailedWithNextRetryAtAfterPublishError() {
        RecordingOutboxStore store = new RecordingOutboxStore(List.of(message("event-1")));
        RecordingPublisher publisher = new RecordingPublisher();
        publisher.fail(new IllegalStateException("publish failed"));
        R2dbcOutboxProperties properties = properties();
        properties.setRetryDelay(Duration.ofSeconds(45));

        StepVerifier.create(scheduler(store, publisher, properties).flushBatch())
                .expectNext(0)
                .verifyComplete();

        assertThat(store.publishedIds()).isEmpty();
        assertThat(store.failedIds()).containsExactly("event-1");
        assertThat(store.nextRetryAt()).isEqualTo(NOW.plusSeconds(45));
    }

    @Test
    void flushTicksDoNotOverlap() {
        RecordingOutboxStore store = new RecordingOutboxStore(List.of(message("event-1")));
        RecordingPublisher publisher = new RecordingPublisher();
        TestPublisher<Void> publishCompletion = TestPublisher.create();
        publisher.publishResult(publishCompletion.mono());
        ReactiveOutboxFlushScheduler scheduler = scheduler(store, publisher, properties());

        StepVerifier.create(scheduler.flushBatch())
                .expectSubscription()
                .then(() -> assertThat(publisher.publishedEventNames()).containsExactly("order.created"))
                .then(() -> StepVerifier.create(scheduler.flushBatch())
                        .expectNext(0)
                        .verifyComplete())
                .then(publishCompletion::complete)
                .expectNext(1)
                .verifyComplete();

        assertThat(store.findPendingCalls()).isEqualTo(1);
    }

    @Test
    void processingUsesBoundedSequentialConcatMap() throws Exception {
        String source = Files.readString(Path.of(
                "src/main/java/io/github/huynhngochuyhoang/reliablemessage/outbox/r2dbc/ReactiveOutboxFlushScheduler.java"
        ));

        assertThat(source)
                .contains(".concatMap(")
                .doesNotContain(".flatMap(message ->");
    }

    @Test
    void flusherDoesNotUseTransportOrJdbcClientsDirectly() throws Exception {
        String source = Files.readString(Path.of(
                "src/main/java/io/github/huynhngochuyhoang/reliablemessage/outbox/r2dbc/ReactiveOutboxFlushScheduler.java"
        ));

        assertThat(source)
                .doesNotContain("RabbitTemplate")
                .doesNotContain("KafkaSender")
                .doesNotContain("JdbcTemplate")
                .doesNotContain("AsyncRabbitTemplate");
    }

    @Test
    void scheduledFlushSubscribesToBatchWhenEnabled() {
        PublisherProbe<Integer> probe = PublisherProbe.of(Mono.just(0));
        ReactiveOutboxFlushScheduler scheduler = new ReactiveOutboxFlushScheduler(
                new RecordingOutboxStore(List.of()),
                new RecordingPublisher(),
                properties(),
                Clock.fixed(NOW, ZoneOffset.UTC)
        ) {
            @Override
            public Mono<Integer> flushBatch() {
                return probe.mono();
            }
        };

        scheduler.flush();

        probe.assertWasSubscribed();
    }

    private static ReactiveOutboxFlushScheduler scheduler(
            ReactiveOutboxStore store,
            ReactiveReliablePublisher publisher,
            R2dbcOutboxProperties properties
    ) {
        return new ReactiveOutboxFlushScheduler(store, publisher, properties, Clock.fixed(NOW, ZoneOffset.UTC));
    }

    private static R2dbcOutboxProperties properties() {
        R2dbcOutboxProperties properties = new R2dbcOutboxProperties();
        properties.setEnabled(true);
        properties.setFlushEnabled(true);
        properties.setBatchSize(10);
        properties.setRetryDelay(Duration.ofSeconds(30));
        return properties;
    }

    private static OutboxMessage message(String id) {
        return new OutboxMessage(
                id,
                "order.created",
                "order-1",
                "idem-" + id,
                "order-1",
                new OrderCreated("order-1"),
                PublishOptions.builder().correlationId("correlation-1").build().headers(),
                MessageStatus.PROCESSING,
                0,
                null,
                NOW,
                null,
                null
        );
    }

    private record OrderCreated(String orderId) {
    }

    private static final class RecordingPublisher implements ReactiveReliablePublisher {
        private final List<String> publishedEventNames = new ArrayList<>();
        private Mono<Void> publishResult = Mono.empty();

        @Override
        public Mono<Void> publish(String eventName, Object payload, PublishOptions options) {
            return Mono.defer(() -> {
                publishedEventNames.add(eventName);
                return publishResult;
            });
        }

        void fail(RuntimeException error) {
            publishResult = Mono.error(error);
        }

        void publishResult(Mono<Void> publishResult) {
            this.publishResult = publishResult;
        }

        List<String> publishedEventNames() {
            return publishedEventNames;
        }
    }

    private static final class RecordingOutboxStore implements ReactiveOutboxStore {
        private final List<OutboxMessage> pending;
        private final List<String> events = new ArrayList<>();
        private final List<String> publishedIds = new ArrayList<>();
        private final List<String> failedIds = new ArrayList<>();
        private int lastLimit;
        private int findPendingCalls;
        private Instant nextRetryAt;

        private RecordingOutboxStore(List<OutboxMessage> pending) {
            this.pending = pending;
        }

        @Override
        public Mono<Void> save(OutboxMessage message) {
            return Mono.empty();
        }

        @Override
        public Flux<OutboxMessage> findPending(int limit) {
            return Flux.defer(() -> {
                findPendingCalls++;
                lastLimit = limit;
                events.add("findPending");
                return Flux.fromIterable(pending);
            });
        }

        @Override
        public Mono<Void> markPublished(String id) {
            return Mono.fromRunnable(() -> {
                events.add("markPublished:" + id);
                publishedIds.add(id);
            });
        }

        @Override
        public Mono<Void> markFailed(String id, Throwable error, Instant nextRetryAt) {
            return Mono.fromRunnable(() -> {
                events.add("markFailed:" + id);
                failedIds.add(id);
                this.nextRetryAt = nextRetryAt;
            });
        }

        List<String> events() {
            return events;
        }

        List<String> publishedIds() {
            return publishedIds;
        }

        List<String> failedIds() {
            return failedIds;
        }

        int lastLimit() {
            return lastLimit;
        }

        int findPendingCalls() {
            return findPendingCalls;
        }

        Instant nextRetryAt() {
            return nextRetryAt;
        }
    }
}
