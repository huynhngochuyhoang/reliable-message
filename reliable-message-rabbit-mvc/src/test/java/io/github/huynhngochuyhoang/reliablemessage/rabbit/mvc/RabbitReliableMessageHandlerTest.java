package io.github.huynhngochuyhoang.reliablemessage.rabbit.mvc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessage;
import io.github.huynhngochuyhoang.reliablemessage.mvc.IdempotencyStartResult;
import io.github.huynhngochuyhoang.reliablemessage.mvc.IdempotencyState;
import io.github.huynhngochuyhoang.reliablemessage.mvc.IdempotencyStore;
import io.github.huynhngochuyhoang.reliablemessage.mvc.ReliableListener;
import io.github.huynhngochuyhoang.reliablemessage.observability.MessageObservability;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import java.lang.reflect.Method;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class RabbitReliableMessageHandlerTest {

    @Test
    void acknowledgesAfterSuccessfulHandlerExecution() throws Exception {
        TestListener listener = new TestListener();
        RabbitReliableMessageHandler handler = handler(listener, "handle");
        Channel channel = org.mockito.Mockito.mock(Channel.class);

        handler.onMessage(message(), channel);

        assertEquals("order-1", listener.lastMessage.payload().orderId());
        verify(channel).basicAck(42L, false);
        verify(channel, never()).basicNack(42L, false, true);
    }

    @Test
    void nacksWhenHandlerFails() throws Exception {
        FailingListener listener = new FailingListener();
        RabbitReliableMessageHandler handler = handler(listener, "handle");
        Channel channel = org.mockito.Mockito.mock(Channel.class);

        assertThrows(IllegalStateException.class, () -> handler.onMessage(message(), channel));

        verify(channel, never()).basicAck(42L, false);
        verify(channel).basicNack(42L, false, true);
    }

    @Test
    void marksIdempotencySuccessAfterSuccessfulHandlerExecution() throws Exception {
        TestListener listener = new TestListener();
        TestIdempotencyStore idempotencyStore = new TestIdempotencyStore(IdempotencyStartResult.startAccepted());
        RabbitReliableMessageHandler handler = handler(listener, "handle", idempotencyStore);
        Channel channel = org.mockito.Mockito.mock(Channel.class);

        handler.onMessage(message(), channel);

        assertEquals("event-1", idempotencyStore.startedKey);
        assertEquals(Duration.ofHours(24), idempotencyStore.ttl);
        assertEquals("event-1", idempotencyStore.succeededKey);
        assertEquals("order-1", listener.lastMessage.payload().orderId());
        verify(channel).basicAck(42L, false);
        verify(channel, never()).basicNack(42L, false, true);
    }

    @Test
    void acknowledgesDuplicateWithoutInvokingHandler() throws Exception {
        TestListener listener = new TestListener();
        TestIdempotencyStore idempotencyStore = new TestIdempotencyStore(
                IdempotencyStartResult.duplicate(IdempotencyState.SUCCESS)
        );
        RabbitReliableMessageHandler handler = handler(listener, "handle", idempotencyStore);
        Channel channel = org.mockito.Mockito.mock(Channel.class);

        handler.onMessage(message(), channel);

        assertEquals("event-1", idempotencyStore.startedKey);
        assertNull(listener.lastMessage);
        verify(channel).basicAck(42L, false);
        verify(channel, never()).basicNack(42L, false, true);
    }


    @Test
    void requeuesInFlightDuplicateWithoutAcknowledging() throws Exception {
        TestListener listener = new TestListener();
        TestIdempotencyStore idempotencyStore = new TestIdempotencyStore(
                IdempotencyStartResult.duplicate(IdempotencyState.PROCESSING)
        );
        RabbitReliableMessageHandler handler = handler(listener, "handle", idempotencyStore);
        Channel channel = org.mockito.Mockito.mock(Channel.class);

        handler.onMessage(message(), channel);

        assertNull(listener.lastMessage);
        verify(channel, never()).basicAck(42L, false);
        verify(channel).basicNack(42L, false, true);
    }

    @Test
    void requeuesFailedDuplicateWithoutAcknowledging() throws Exception {
        TestListener listener = new TestListener();
        TestIdempotencyStore idempotencyStore = new TestIdempotencyStore(
                IdempotencyStartResult.duplicate(IdempotencyState.FAILED)
        );
        RabbitReliableMessageHandler handler = handler(listener, "handle", idempotencyStore);
        Channel channel = org.mockito.Mockito.mock(Channel.class);

        handler.onMessage(message(), channel);

        assertNull(listener.lastMessage);
        verify(channel, never()).basicAck(42L, false);
        verify(channel).basicNack(42L, false, true);
    }

    @Test
    void invalidPayloadFollowsFailurePathWithoutInvokingHandler() throws Exception {
        TestListener listener = new TestListener();
        RabbitReliableMessageHandler handler = handler(listener, "handle");
        Channel channel = org.mockito.Mockito.mock(Channel.class);
        MessageProperties properties = new MessageProperties();
        properties.setDeliveryTag(42L);
        Message invalid = new Message("not-json".getBytes(java.nio.charset.StandardCharsets.UTF_8), properties);

        assertThrows(RuntimeException.class, () -> handler.onMessage(invalid, channel));

        assertNull(listener.lastMessage);
        verify(channel, never()).basicAck(42L, false);
        verify(channel).basicNack(42L, false, true);
    }

    @Test
    void marksIdempotencyFailedWhenHandlerFails() throws Exception {
        FailingListener listener = new FailingListener();
        TestIdempotencyStore idempotencyStore = new TestIdempotencyStore(IdempotencyStartResult.startAccepted());
        RabbitReliableMessageHandler handler = handler(listener, "handle", idempotencyStore);
        Channel channel = org.mockito.Mockito.mock(Channel.class);

        assertThrows(IllegalStateException.class, () -> handler.onMessage(message(), channel));

        assertEquals("event-1", idempotencyStore.failedKey);
        verify(channel, never()).basicAck(42L, false);
        verify(channel).basicNack(42L, false, true);
    }

    @Test
    void returnsNormallyAfterSuccessfulRetryRouting() throws Exception {
        FailingListener listener = new FailingListener();
        RabbitTemplate rabbitTemplate = org.mockito.Mockito.mock(RabbitTemplate.class);
        RabbitRetryStrategy retryStrategy = new RabbitRetryStrategy(
                rabbitTemplate,
                new RabbitReliableMessageProperties(),
                new SimpleMeterRegistry()
        );
        RabbitReliableMessageHandler handler = handler(listener, "handle", null, retryStrategy);
        Channel channel = org.mockito.Mockito.mock(Channel.class);

        assertDoesNotThrow(() -> handler.onMessage(message(), channel));

        verify(channel).basicAck(42L, false);
        verify(channel, never()).basicNack(42L, false, true);
    }

    private static RabbitReliableMessageHandler handler(Object listener, String methodName) throws NoSuchMethodException {
        return handler(listener, methodName, null);
    }

    private static RabbitReliableMessageHandler handler(
            Object listener,
            String methodName,
            IdempotencyStore idempotencyStore
    ) throws NoSuchMethodException {
        return handler(listener, methodName, idempotencyStore, null);
    }

    private static RabbitReliableMessageHandler handler(
            Object listener,
            String methodName,
            IdempotencyStore idempotencyStore,
            RabbitRetryStrategy retryStrategy
    ) throws NoSuchMethodException {
        Method method = listener.getClass().getDeclaredMethod(methodName, ReliableMessage.class);
        RabbitReliableListenerEndpoint endpoint = new RabbitReliableListenerEndpoint(
                "listener",
                listener,
                method,
                "order.created",
                "order-service.order.created",
                OrderCreated.class
        );
        return new RabbitReliableMessageHandler(
                endpoint,
                new JacksonReliableMessageSerializer(new ObjectMapper()),
                new MessageObservability(new SimpleMeterRegistry(), io.micrometer.observation.ObservationRegistry.NOOP),
                idempotencyStore,
                Duration.ofHours(24),
                retryStrategy
        );
    }

    private static Message message() {
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
        MessageProperties properties = new MessageProperties();
        properties.setDeliveryTag(42L);
        return new Message(body, properties);
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
        private Duration ttl;
        private String succeededKey;
        private String failedKey;

        TestIdempotencyStore(IdempotencyStartResult startResult) {
            this.startResult = startResult;
        }

        @Override
        public IdempotencyStartResult tryStart(String key, Duration ttl) {
            this.startedKey = key;
            this.ttl = ttl;
            return startResult;
        }

        @Override
        public void markSuccess(String key) {
            this.succeededKey = key;
        }

        @Override
        public void markFailed(String key, Throwable error) {
            this.failedKey = key;
        }
    }
}
