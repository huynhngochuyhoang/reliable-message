package io.github.huynhngochuyhoang.reliablemessage.kafka.webflux;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.huynhngochuyhoang.reliablemessage.core.PublishOptions;
import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessage;
import io.github.huynhngochuyhoang.reliablemessage.observability.MessageObservability;
import io.github.huynhngochuyhoang.reliablemessage.webflux.IdempotencyStartResult;
import io.github.huynhngochuyhoang.reliablemessage.webflux.IdempotencyState;
import io.github.huynhngochuyhoang.reliablemessage.webflux.ReactiveIdempotencyStore;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.observation.ObservationRegistry;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;
import reactor.test.StepVerifier;

import java.lang.reflect.Method;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

class ReactiveKafkaObservabilityStabilizationTest {

    @Test
    void recordsPublishSuccessAndFailureWithWebFluxKafkaTags() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        StepVerifier.create(publisher(successfulSender(), registry)
                        .publish("order.created", new OrderCreated("order-1"), PublishOptions.empty()))
                .verifyComplete();

        assertEquals(1.0, registry.get("message_publish_total")
                .tags("runtime", "webflux", "transport", "kafka", "event_name", "order.created", "status", "success")
                .counter().count());

        @SuppressWarnings("unchecked")
        KafkaSender<String, byte[]> failingSender = org.mockito.Mockito.mock(KafkaSender.class);
        when(failingSender.send(any(org.reactivestreams.Publisher.class)))
                .thenReturn(Flux.error(new IllegalStateException("broker unavailable")));

        StepVerifier.create(publisher(failingSender, registry)
                        .publish("order.failed", new OrderCreated("order-2"), PublishOptions.empty()))
                .expectErrorMessage("broker unavailable")
                .verify();

        assertEquals(1.0, registry.get("message_publish_total")
                .tags("runtime", "webflux", "transport", "kafka", "event_name", "order.failed", "status", "failed")
                .counter().count());
        assertEquals(1.0, registry.get("message_publish_failed_total")
                .tags("runtime", "webflux", "transport", "kafka", "event_name", "order.failed", "status", "failed")
                .counter().count());
    }

    @Test
    void recordsConsumeSuccessFailureAndDuplicateWithoutChangingSignals() throws Exception {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        MessageObservability observability = new MessageObservability(registry, ObservationRegistry.NOOP);
        AtomicBoolean committed = new AtomicBoolean();

        StepVerifier.create(handler(null, null, observability)
                        .handle(record(committed), endpoint(new CompletingListener())))
                .verifyComplete();

        assertEquals(true, committed.get());
        assertEquals(1.0, registry.get("message_consume_total")
                .tags("runtime", "webflux", "transport", "kafka", "event_name", "order.created", "status", "success")
                .counter().count());

        ReactiveKafkaRetryStrategy retryStrategy = org.mockito.Mockito.mock(ReactiveKafkaRetryStrategy.class);
        when(retryStrategy.routeFailure(any(), any(), any())).thenReturn(Mono.empty());
        StepVerifier.create(handler(null, retryStrategy, observability)
                        .handle(record(new AtomicBoolean()), endpoint(new FailingListener())))
                .verifyComplete();

        assertEquals(1.0, registry.get("message_consume_failed_total")
                .tags("runtime", "webflux", "transport", "kafka", "event_name", "order.created", "status", "failed")
                .counter().count());

        ReactiveIdempotencyStore store = org.mockito.Mockito.mock(ReactiveIdempotencyStore.class);
        when(store.tryStart("event-1", Duration.ofHours(1)))
                .thenReturn(Mono.just(IdempotencyStartResult.duplicate(IdempotencyState.SUCCESS)));
        CompletingListener duplicateListener = new CompletingListener();

        StepVerifier.create(handler(store, null, observability)
                        .handle(record(new AtomicBoolean()), endpoint(duplicateListener)))
                .verifyComplete();

        assertEquals(0, duplicateListener.invocations);
        assertEquals(1.0, registry.get("message_duplicate_total")
                .tags("runtime", "webflux", "transport", "kafka", "event_name", "order.created", "status", "duplicate")
                .counter().count());
    }


    
    @Test
    void metricFailureDoesNotAlterPublishOrConsumeSignals() throws Exception {
        MessageObservability broken = org.mockito.Mockito.mock(MessageObservability.class);
        org.mockito.Mockito.doThrow(new IllegalStateException("metrics unavailable"))
                .when(broken).increment(any(), any());

        ReactiveKafkaReliableMessageProperties properties = new ReactiveKafkaReliableMessageProperties();
        ReactiveKafkaReliablePublisher publisher = new ReactiveKafkaReliablePublisher(
                successfulSender(),
                serializer(),
                properties,
                Clock.fixed(Instant.parse("2026-05-18T00:00:00Z"), ZoneOffset.UTC),
                broken
        );
        StepVerifier.create(publisher.publish("order.created", new OrderCreated("order-1"), PublishOptions.empty()))
                .verifyComplete();

        StepVerifier.create(handler(null, null, broken)
                        .handle(record(new AtomicBoolean()), endpoint(new CompletingListener())))
                .verifyComplete();
    }

    private static ReactiveKafkaReliablePublisher publisher(
            KafkaSender<String, byte[]> sender,
            SimpleMeterRegistry registry
    ) {
        ReactiveKafkaReliableMessageProperties properties = new ReactiveKafkaReliableMessageProperties();
        properties.getKafka().setTopicPrefix("app.");
        return new ReactiveKafkaReliablePublisher(
                sender,
                serializer(),
                properties,
                Clock.fixed(Instant.parse("2026-05-18T00:00:00Z"), ZoneOffset.UTC),
                new MessageObservability(registry, ObservationRegistry.NOOP)
        );
    }

    private static ReactiveKafkaReliableMessageHandler handler(
            ReactiveIdempotencyStore store,
            ReactiveKafkaRetryStrategy retryStrategy,
            MessageObservability observability
    ) {
        return new ReactiveKafkaReliableMessageHandler(
                serializer(),
                store,
                Duration.ofHours(1),
                retryStrategy,
                observability
        );
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static KafkaSender<String, byte[]> successfulSender() {
        KafkaSender<String, byte[]> sender = org.mockito.Mockito.mock(KafkaSender.class);
        when(sender.send(any(org.reactivestreams.Publisher.class))).thenAnswer(invocation ->
                Flux.from(invocation.<org.reactivestreams.Publisher<SenderRecord<String, byte[], String>>>getArgument(0))
                        .map(record -> {
                            SenderResult<String> result = org.mockito.Mockito.mock(SenderResult.class);
                            when(result.correlationMetadata()).thenReturn(record.correlationMetadata());
                            return result;
                        })
        );
        return sender;
    }

    private static ReactiveKafkaReliableListenerEndpoint endpoint(Object listener) throws Exception {
        Method method = listener.getClass().getDeclaredMethod("handle", ReliableMessage.class);
        return new ReactiveKafkaReliableListenerEndpoint(
                "listener", listener, method, "order.created", "app.order.created", "orders", OrderCreated.class
        );
    }

    private static TestRecord record(AtomicBoolean committed) {
        ReliableMessage<OrderCreated> message = new ReliableMessage<>(
                "message-1", "order.created", "order-1", "event-1", "correlation-1",
                null, Instant.now(), Map.of(), new OrderCreated("order-1")
        );
        return new TestRecord(serializer().serialize(message), committed);
    }

    private static JacksonReliableMessageSerializer serializer() {
        return new JacksonReliableMessageSerializer(new ObjectMapper());
    }

    static final class CompletingListener {
        private int invocations;

        Mono<Void> handle(ReliableMessage<OrderCreated> message) {
            invocations++;
            return Mono.empty();
        }
    }

    static final class FailingListener {
        Mono<Void> handle(ReliableMessage<OrderCreated> message) {
            return Mono.error(new IllegalStateException("handler failed"));
        }
    }

    private record TestRecord(byte[] value, AtomicBoolean committed) implements ReactiveKafkaReceivedRecord {
        @Override
        public String topic() {
            return "app.order.created";
        }

        @Override
        public String key() {
            return "order-1";
        }

        @Override
        public Headers headers() {
            return new RecordHeaders();
        }

        @Override
        public ReactiveKafkaReceiverOffset receiverOffset() {
            return () -> Mono.fromRunnable(() -> committed.set(true));
        }
    }

    record OrderCreated(String orderId) {
    }
}
