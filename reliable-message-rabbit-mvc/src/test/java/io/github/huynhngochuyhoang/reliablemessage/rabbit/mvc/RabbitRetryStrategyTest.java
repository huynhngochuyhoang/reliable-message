package io.github.huynhngochuyhoang.reliablemessage.rabbit.mvc;

import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessage;
import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessageHeaders;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentCaptor.forClass;
import static org.mockito.Mockito.verify;

class RabbitRetryStrategyTest {

    @Test
    void routesFailedMessageToNextRetryQueueWithIncrementedRetryCount() throws Exception {
        RabbitTemplate rabbitTemplate = org.mockito.Mockito.mock(RabbitTemplate.class);
        RabbitRetryStrategy strategy = new RabbitRetryStrategy(rabbitTemplate, properties(), new SimpleMeterRegistry());

        strategy.routeFailure(message(0), endpoint(), new IllegalStateException("boom"));

        org.mockito.ArgumentCaptor<Message> messageCaptor = forClass(Message.class);
        verify(rabbitTemplate).send(
                org.mockito.Mockito.eq("app.events"),
                org.mockito.Mockito.eq("order-service.order.created.retry.5s"),
                messageCaptor.capture()
        );
        assertEquals(1, ((Number) messageCaptor.getValue()
                .getMessageProperties()
                .getHeader(ReliableMessageHeaders.RETRY_COUNT)).intValue());
        assertEquals("message-1", messageCaptor.getValue()
                .getMessageProperties()
                .getHeader(ReliableMessageHeaders.ORIGINAL_MESSAGE_ID));
    }

    @Test
    void routesMessageToDlqWhenAttemptsAreExhausted() throws Exception {
        RabbitTemplate rabbitTemplate = org.mockito.Mockito.mock(RabbitTemplate.class);
        RabbitRetryStrategy strategy = new RabbitRetryStrategy(rabbitTemplate, properties(), new SimpleMeterRegistry());

        strategy.routeFailure(message(2), endpoint(), new IllegalStateException("boom"));

        org.mockito.ArgumentCaptor<Message> messageCaptor = forClass(Message.class);
        verify(rabbitTemplate).send(
                org.mockito.Mockito.eq("app.events"),
                org.mockito.Mockito.eq("order-service.order.created.dlq"),
                messageCaptor.capture()
        );
        assertEquals(3, ((Number) messageCaptor.getValue()
                .getMessageProperties()
                .getHeader(ReliableMessageHeaders.RETRY_COUNT)).intValue());
    }

    private static RabbitReliableMessageProperties properties() {
        RabbitReliableMessageProperties properties = new RabbitReliableMessageProperties();
        properties.setServiceName("order-service");
        properties.getRetry().setAttempts(3);
        properties.getRetry().setBackoff(List.of(Duration.ofSeconds(5), Duration.ofMinutes(1)));
        return properties;
    }

    private static RabbitReliableListenerEndpoint endpoint() throws NoSuchMethodException {
        Method method = TestListener.class.getDeclaredMethod("handle", ReliableMessage.class);
        return new RabbitReliableListenerEndpoint(
                "listener",
                new TestListener(),
                method,
                "order.created",
                "order-service.order.created",
                Object.class
        );
    }

    private static Message message(int retryCount) {
        MessageProperties properties = new MessageProperties();
        properties.setHeader(ReliableMessageHeaders.MESSAGE_ID, "message-1");
        properties.setHeader(ReliableMessageHeaders.RETRY_COUNT, retryCount);
        return new Message("{}".getBytes(java.nio.charset.StandardCharsets.UTF_8), properties);
    }

    static final class TestListener {

        void handle(ReliableMessage<Object> message) {
        }
    }
}
