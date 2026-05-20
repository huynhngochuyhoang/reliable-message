package io.github.huynhngochuyhoang.reliablemessage.kafka.webflux;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessage;
import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessageHeaders;
import io.github.huynhngochuyhoang.reliablemessage.webflux.IdempotencyStartResult;
import io.github.huynhngochuyhoang.reliablemessage.webflux.ReactiveIdempotencyStore;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

import java.lang.reflect.Method;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ReactiveKafkaReliableMessageHandlerTest {

    @Test
    void commitsOffsetOnlyAfterHandlerMonoCompletes() throws Exception {
        Listener listener = new Listener();
        ReactiveKafkaReliableListenerEndpoint endpoint = endpoint(listener);
        AtomicBoolean committed = new AtomicBoolean(false);
        ReactiveKafkaReliableMessageHandler handler = new ReactiveKafkaReliableMessageHandler(
                serializer(),
                null,
                Duration.ofHours(1),
                null
        );

        StepVerifier.create(handler.handle(record(committed), endpoint))
                .then(() -> assertFalse(committed.get()))
                .then(() -> listener.completion.tryEmitEmpty())
                .verifyComplete();

        assertTrue(committed.get());
    }

    @Test
    void duplicateCommitsOffsetWithoutInvokingHandler() throws Exception {
        Listener listener = new Listener();
        ReactiveIdempotencyStore idempotencyStore = org.mockito.Mockito.mock(ReactiveIdempotencyStore.class);
        when(idempotencyStore.tryStart("event-1", Duration.ofHours(1)))
                .thenReturn(Mono.just(IdempotencyStartResult.duplicate(io.github.huynhngochuyhoang.reliablemessage.webflux.IdempotencyState.SUCCESS)));
        AtomicBoolean committed = new AtomicBoolean(false);
        ReactiveKafkaReliableMessageHandler handler = new ReactiveKafkaReliableMessageHandler(
                serializer(),
                idempotencyStore,
                Duration.ofHours(1),
                null
        );

        StepVerifier.create(handler.handle(record(committed), endpoint(listener)))
                .verifyComplete();

        assertTrue(committed.get());
        assertFalse(listener.invoked);
    }

    @Test
    void failureRoutesAndCommitsThenCompletes() throws Exception {
        FailingListener listener = new FailingListener();
        ReactiveKafkaRetryStrategy retryStrategy = org.mockito.Mockito.mock(ReactiveKafkaRetryStrategy.class);
        when(retryStrategy.routeFailure(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any()))
                .thenReturn(Mono.empty());
        AtomicBoolean committed = new AtomicBoolean(false);
        ReactiveKafkaReliableMessageHandler handler = new ReactiveKafkaReliableMessageHandler(
                serializer(),
                null,
                Duration.ofHours(1),
                retryStrategy
        );

        StepVerifier.create(handler.handle(record(committed), endpoint(listener)))
                .verifyComplete();

        assertTrue(committed.get());
        verify(retryStrategy).routeFailure(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any());
    }

    private static ReactiveKafkaReliableListenerEndpoint endpoint(Object listener) throws Exception {
        Method method = listener.getClass().getDeclaredMethod("handle", ReliableMessage.class);
        return new ReactiveKafkaReliableListenerEndpoint(
                "listener",
                listener,
                method,
                "order.created",
                "app.order.created",
                "order-service",
                OrderCreated.class
        );
    }

    private static TestRecord record(AtomicBoolean committed) {
        ReliableMessage<OrderCreated> message = new ReliableMessage<>(
                "message-1",
                "order.created",
                "order-1",
                "event-1",
                "correlation-1",
                "trace-1",
                Instant.now(),
                Map.of(ReliableMessageHeaders.MESSAGE_ID, "message-1"),
                new OrderCreated("order-1")
        );
        return new TestRecord(serializer().serialize(message), committed);
    }

    private static JacksonReliableMessageSerializer serializer() {
        return new JacksonReliableMessageSerializer(new ObjectMapper());
    }

    static class Listener {
        private final Sinks.Empty<Void> completion = Sinks.empty();
        private boolean invoked;

        Mono<Void> handle(ReliableMessage<OrderCreated> message) {
            invoked = true;
            return completion.asMono();
        }
    }

    static final class FailingListener {
        Mono<Void> handle(ReliableMessage<OrderCreated> message) {
            return Mono.error(new IllegalStateException("boom"));
        }
    }

    record OrderCreated(String orderId) {
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
}
