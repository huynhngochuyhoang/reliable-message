package io.github.huynhngochuyhoang.reliablemessage.rabbit.mvc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessage;
import io.github.huynhngochuyhoang.reliablemessage.mvc.ReliableListener;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;

import java.lang.reflect.Method;
import java.time.Instant;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
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

    private static RabbitReliableMessageHandler handler(Object listener, String methodName) throws NoSuchMethodException {
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
                new SimpleMeterRegistry()
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
}
