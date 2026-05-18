package io.github.huynhngochuyhoang.reliablemessage.kafka.mvc;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessage;
import io.github.huynhngochuyhoang.reliablemessage.mvc.IdempotencyStartResult;
import io.github.huynhngochuyhoang.reliablemessage.mvc.IdempotencyState;
import io.github.huynhngochuyhoang.reliablemessage.mvc.IdempotencyStore;
import io.github.huynhngochuyhoang.reliablemessage.mvc.ReliableListener;
import io.github.huynhngochuyhoang.reliablemessage.observability.MessageObservability;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.observation.ObservationRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;

import java.lang.reflect.Method;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class KafkaReliableMessageHandlerTest {

    @Test
    void commitsOffsetAfterSuccessfulHandlerExecution() throws Exception {
        TestListener listener = new TestListener();
        KafkaReliableMessageHandler handler = handler(listener, "handle", null, null);
        Acknowledgment acknowledgment = org.mockito.Mockito.mock(Acknowledgment.class);

        handler.onMessage(record(), acknowledgment);

        assertEquals("order-1", listener.lastMessage.payload().orderId());
        verify(acknowledgment).acknowledge();
    }

    @Test
    void doesNotCommitOffsetWhenHandlerFailsWithoutRetryStrategy() throws Exception {
        FailingListener listener = new FailingListener();
        KafkaReliableMessageHandler handler = handler(listener, "handle", null, null);
        Acknowledgment acknowledgment = org.mockito.Mockito.mock(Acknowledgment.class);

        assertThrows(IllegalStateException.class, () -> handler.onMessage(record(), acknowledgment));

        verify(acknowledgment, never()).acknowledge();
    }

    @Test
    void routesFailureToRetryTopicAndCommitsOffset() throws Exception {
        FailingListener listener = new FailingListener();
        KafkaTemplate<String, byte[]> kafkaTemplate = kafkaTemplate();
        KafkaRetryStrategy retryStrategy = new KafkaRetryStrategy(
                kafkaTemplate,
                new KafkaReliableMessageProperties(),
                new SimpleMeterRegistry()
        );
        KafkaReliableMessageHandler handler = handler(listener, "handle", null, retryStrategy);
        Acknowledgment acknowledgment = org.mockito.Mockito.mock(Acknowledgment.class);

        handler.onMessage(record(), acknowledgment);

        ArgumentCaptor<ProducerRecord<String, byte[]>> recordCaptor = producerRecordCaptor();
        verify(kafkaTemplate).send(recordCaptor.capture());
        assertEquals("app.order.created.order-service.retry.5s", recordCaptor.getValue().topic());
        verify(acknowledgment).acknowledge();
    }

    @Test
    void acknowledgesDuplicateWithoutInvokingHandler() throws Exception {
        TestListener listener = new TestListener();
        TestIdempotencyStore idempotencyStore = new TestIdempotencyStore(
                IdempotencyStartResult.duplicate(IdempotencyState.SUCCESS)
        );
        KafkaReliableMessageHandler handler = handler(listener, "handle", idempotencyStore, null);
        Acknowledgment acknowledgment = org.mockito.Mockito.mock(Acknowledgment.class);

        handler.onMessage(record(), acknowledgment);

        assertEquals("event-1", idempotencyStore.startedKey);
        assertNull(listener.lastMessage);
        verify(acknowledgment).acknowledge();
    }

    private static KafkaReliableMessageHandler handler(
            Object listener,
            String methodName,
            IdempotencyStore idempotencyStore,
            KafkaRetryStrategy retryStrategy
    ) throws NoSuchMethodException {
        Method method = listener.getClass().getDeclaredMethod(methodName, ReliableMessage.class);
        KafkaReliableListenerEndpoint endpoint = new KafkaReliableListenerEndpoint(
                "listener",
                listener,
                method,
                "order.created",
                "app.order.created",
                "order-service",
                OrderCreated.class
        );
        return new KafkaReliableMessageHandler(
                endpoint,
                new JacksonReliableMessageSerializer(new ObjectMapper()),
                new MessageObservability(new SimpleMeterRegistry(), ObservationRegistry.NOOP),
                idempotencyStore,
                Duration.ofHours(24),
                retryStrategy
        );
    }

    private static ConsumerRecord<String, byte[]> record() {
        ReliableMessage<OrderCreated> reliableMessage = new ReliableMessage<>(
                "message-1",
                "order.created",
                "order-1",
                "event-1",
                "correlation-1",
                "trace-1",
                Instant.parse("2026-05-17T00:00:00Z"),
                Map.of(),
                new OrderCreated("order-1")
        );
        byte[] body = new JacksonReliableMessageSerializer(new ObjectMapper()).serialize(reliableMessage);
        return new ConsumerRecord<>("app.order.created", 0, 42L, "order-1", body);
    }

    @SuppressWarnings("unchecked")
    private static KafkaTemplate<String, byte[]> kafkaTemplate() {
        KafkaTemplate<String, byte[]> kafkaTemplate = org.mockito.Mockito.mock(KafkaTemplate.class);
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.completedFuture(null));
        return kafkaTemplate;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static ArgumentCaptor<ProducerRecord<String, byte[]>> producerRecordCaptor() {
        return (ArgumentCaptor) ArgumentCaptor.forClass(ProducerRecord.class);
    }

    static final class TestListener {
        ReliableMessage<OrderCreated> lastMessage;

        @ReliableListener("order.created")
        void handle(ReliableMessage<OrderCreated> message) {
            this.lastMessage = message;
        }
    }

    static final class FailingListener {

        @ReliableListener("order.created")
        void handle(ReliableMessage<OrderCreated> message) {
            throw new IllegalStateException("boom");
        }
    }

    record OrderCreated(String orderId) {
    }

    static final class TestIdempotencyStore implements IdempotencyStore {
        private final IdempotencyStartResult startResult;
        private String startedKey;

        TestIdempotencyStore(IdempotencyStartResult startResult) {
            this.startResult = startResult;
        }

        @Override
        public IdempotencyStartResult tryStart(String key, Duration ttl) {
            this.startedKey = key;
            return startResult;
        }

        @Override
        public void markSuccess(String key) {
        }

        @Override
        public void markFailed(String key, Throwable error) {
        }
    }
}
