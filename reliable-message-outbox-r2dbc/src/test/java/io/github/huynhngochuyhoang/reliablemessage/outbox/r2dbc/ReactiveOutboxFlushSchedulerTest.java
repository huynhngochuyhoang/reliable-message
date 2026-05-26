package io.github.huynhngochuyhoang.reliablemessage.outbox.r2dbc;

import io.github.huynhngochuyhoang.reliablemessage.core.MessageStatus;
import io.github.huynhngochuyhoang.reliablemessage.core.PublishOptions;
import io.github.huynhngochuyhoang.reliablemessage.webflux.OutboxMessage;
import io.github.huynhngochuyhoang.reliablemessage.webflux.ReactiveOutboxStore;
import io.github.huynhngochuyhoang.reliablemessage.webflux.ReactiveReliablePublisher;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ExtendWith(OutputCaptureExtension.class)
class ReactiveOutboxFlushSchedulerTest {

    private static final Instant NOW = Instant.parse("2026-05-18T00:00:00Z");

    @Test
    void readsPendingRecordsWithConfiguredBatchSizeAndProcessesBoundedConcurrent() {
        RecordingOutboxStore store = new RecordingOutboxStore(List.of(
                message("event-1"),
                message("event-2"),
                message("event-3")
        ));
        RecordingPublisher publisher = new RecordingPublisher();
        R2dbcOutboxProperties properties = properties();
        properties.setBatchSize(25);

        StepVerifier.create(scheduler(store, publisher, properties).flushBatch())
                .expectNext(3)
                .verifyComplete();

        assertThat(store.lastLimit()).isEqualTo(25);
        assertThat(store.publishedIds()).containsExactly("event-1", "event-2", "event-3");
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
    void hungPublishTimesOutAndReleasesRunningGuard() {
        RecordingOutboxStore store = new RecordingOutboxStore(List.of(message("event-1")));
        RecordingPublisher publisher = new RecordingPublisher();
        publisher.publishResult(Mono.never());
        R2dbcOutboxProperties properties = properties();
        properties.setPublishTimeout(Duration.ofMillis(10));
        ReactiveOutboxFlushScheduler scheduler = scheduler(store, publisher, properties);

        StepVerifier.create(scheduler.flushBatch())
                .expectNext(0)
                .verifyComplete();

        assertThat(store.failedIds()).containsExactly("event-1");
        assertThat(store.failedErrors()).singleElement().isInstanceOf(TimeoutException.class);

        publisher.publishResult(Mono.empty());

        StepVerifier.create(scheduler.flushBatch())
                .expectNext(1)
                .verifyComplete();

        assertThat(store.findPendingCalls()).isEqualTo(2);
        assertThat(store.publishedIds()).containsExactly("event-1");
    }

    @Test
    void hungFindPendingTimesOutAndReleasesRunningGuard() {
        RecordingOutboxStore store = new RecordingOutboxStore(List.of(message("event-1")));
        store.findPendingResult(Flux.never());
        RecordingPublisher publisher = new RecordingPublisher();
        R2dbcOutboxProperties properties = properties();
        properties.setPublishTimeout(Duration.ofMillis(10));
        ReactiveOutboxFlushScheduler scheduler = scheduler(store, publisher, properties);

        StepVerifier.create(scheduler.flushBatch())
                .expectError(TimeoutException.class)
                .verify();

        store.findPendingResult(null);

        StepVerifier.create(scheduler.flushBatch())
                .expectNext(1)
                .verifyComplete();
    }


    @Test
    void hungMarkPublishedTimesOutAndReleasesRunningGuard() {
        RecordingOutboxStore store = new RecordingOutboxStore(List.of(message("event-1")));
        store.markPublishedResult(Mono.never());
        RecordingPublisher publisher = new RecordingPublisher();
        R2dbcOutboxProperties properties = properties();
        properties.setPublishTimeout(Duration.ofMillis(10));
        ReactiveOutboxFlushScheduler scheduler = scheduler(store, publisher, properties);

        StepVerifier.create(scheduler.flushBatch())
                .expectNext(0)
                .verifyComplete();

        assertThat(store.failedIds()).containsExactly("event-1");
        assertThat(store.failedErrors()).singleElement().isInstanceOf(TimeoutException.class);

        store.markPublishedResult(null);

        StepVerifier.create(scheduler.flushBatch())
                .expectNext(1)
                .verifyComplete();
    }

    @Test
    void hungMarkFailedTimesOutWithoutAbortingBatchAndReleasesRunningGuard() {
        RecordingOutboxStore store = new RecordingOutboxStore(List.of(message("event-1")));
        store.markFailedResult(Mono.never());
        RecordingPublisher publisher = new RecordingPublisher();
        publisher.fail(new IllegalStateException("publish failed"));
        R2dbcOutboxProperties properties = properties();
        properties.setPublishTimeout(Duration.ofMillis(10));
        ReactiveOutboxFlushScheduler scheduler = scheduler(store, publisher, properties);

        StepVerifier.create(scheduler.flushBatch())
                .expectNext(0)
                .verifyComplete();

        store.markFailedResult(null);
        publisher.publishResult(Mono.empty());

        StepVerifier.create(scheduler.flushBatch())
                .expectNext(1)
                .verifyComplete();
    }

    @Test
    void oneItemFailureDoesNotAbortConcurrentBatchFlush() {
        RecordingOutboxStore store = new RecordingOutboxStore(List.of(message("event-1"), message("event-2")));
        store.markPublishedResult("event-1", Mono.error(new IllegalStateException("mark published failed")));
        store.markFailedResult("event-1", Mono.error(new IllegalStateException("mark failed failed")));
        RecordingPublisher publisher = new RecordingPublisher();
        R2dbcOutboxProperties properties = properties();
        properties.setBatchSize(2);

        StepVerifier.create(scheduler(store, publisher, properties).flushBatch())
                .expectNext(1)
                .verifyComplete();

        assertThat(store.failedIds()).containsExactly("event-1");
        assertThat(store.publishedIds()).containsExactly("event-2");
    }


    @Test
    void rejectsNonPositivePublishTimeout() {
        R2dbcOutboxProperties properties = new R2dbcOutboxProperties();

        assertThatThrownBy(() -> properties.setPublishTimeout(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("publishTimeout must be positive");
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
    void processingUsesBoundedFlatMapWithoutPreclaimedList() throws Exception {
        String source = Files.readString(Path.of(
                "src/main/java/io/github/huynhngochuyhoang/reliablemessage/outbox/r2dbc/ReactiveOutboxFlushScheduler.java"
        ));

        assertThat(source)
                .contains(".flatMap(this::publishAndMarkSafely, properties.getBatchSize())")
                .doesNotContain(".flatMap(message ->")
                .doesNotContain(".collectList(")
                .doesNotContain(".concatMap(");
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


    @Test
    void scheduledFlushLogsBatchFailures(CapturedOutput output) {
        ReactiveOutboxFlushScheduler scheduler = new ReactiveOutboxFlushScheduler(
                new RecordingOutboxStore(List.of()),
                new RecordingPublisher(),
                properties(),
                Clock.fixed(NOW, ZoneOffset.UTC)
        ) {
            @Override
            public Mono<Integer> flushBatch() {
                return Mono.error(new IllegalStateException("database unavailable"));
            }
        };

        scheduler.flush();

        assertThat(output).contains("Reliable message R2DBC outbox flush failed")
                .contains("database unavailable");
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
        properties.setPublishTimeout(Duration.ofSeconds(30));
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
        private final List<Throwable> failedErrors = new ArrayList<>();
        private Flux<OutboxMessage> findPendingResult;
        private Mono<Void> markPublishedResult;
        private Mono<Void> markFailedResult;
        private final Map<String, Mono<Void>> markPublishedResultsById = new HashMap<>();
        private final Map<String, Mono<Void>> markFailedResultsById = new HashMap<>();
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
                if (findPendingResult != null) {
                    return findPendingResult;
                }
                return Flux.fromIterable(pending).take(limit);
            });
        }

        @Override
        public Mono<Void> markPublished(String id) {
            return Mono.defer(() -> {
                events.add("markPublished:" + id);
                Mono<Void> result = markPublishedResultsById.getOrDefault(id, markPublishedResult);
                if (result == null) {
                    result = Mono.empty();
                }
                return result.then(Mono.fromRunnable(() -> publishedIds.add(id)));
            });
        }

        @Override
        public Mono<Void> markFailed(String id, Throwable error, Instant nextRetryAt) {
            return Mono.defer(() -> {
                events.add("markFailed:" + id);
                failedIds.add(id);
                failedErrors.add(error);
                this.nextRetryAt = nextRetryAt;
                Mono<Void> result = markFailedResultsById.getOrDefault(id, markFailedResult);
                if (result == null) {
                    result = Mono.empty();
                }
                return result;
            });
        }

        void findPendingResult(Flux<OutboxMessage> findPendingResult) {
            this.findPendingResult = findPendingResult;
        }

        void markPublishedResult(Mono<Void> markPublishedResult) {
            this.markPublishedResult = markPublishedResult;
        }

        void markPublishedResult(String id, Mono<Void> markPublishedResult) {
            this.markPublishedResultsById.put(id, markPublishedResult);
        }

        void markFailedResult(Mono<Void> markFailedResult) {
            this.markFailedResult = markFailedResult;
        }

        void markFailedResult(String id, Mono<Void> markFailedResult) {
            this.markFailedResultsById.put(id, markFailedResult);
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

        List<Throwable> failedErrors() {
            return failedErrors;
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
