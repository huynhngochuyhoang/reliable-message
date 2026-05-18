package io.github.huynhngochuyhoang.reliablemessage.rabbit.mvc;

import io.github.huynhngochuyhoang.reliablemessage.core.DeadLetterRecord;
import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessageHeaders;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentCaptor.forClass;
import static org.mockito.Mockito.verify;

class RabbitDlqServiceTest {

    @Test
    void retryRepublishesDlqMessageToMainRoute() {
        RabbitTemplate rabbitTemplate = org.mockito.Mockito.mock(RabbitTemplate.class);
        RabbitDlqService dlqService = new RabbitDlqService(
                rabbitTemplate,
                properties(),
                clock(),
                new SimpleMeterRegistry()
        );

        dlqService.retry("order.created", message());

        org.mockito.ArgumentCaptor<Message> messageCaptor = forClass(Message.class);
        verify(rabbitTemplate).send(
                org.mockito.Mockito.eq("app.events"),
                org.mockito.Mockito.eq("order.created"),
                messageCaptor.capture()
        );
        assertEquals(0, ((Number) messageCaptor.getValue()
                .getMessageProperties()
                .getHeader(ReliableMessageHeaders.RETRY_COUNT)).intValue());
    }

    @Test
    void discardReturnsDeadLetterRecordForIntentionalDiscard() {
        RabbitDlqService dlqService = new RabbitDlqService(
                org.mockito.Mockito.mock(RabbitTemplate.class),
                properties(),
                clock(),
                new SimpleMeterRegistry()
        );

        DeadLetterRecord record = dlqService.discard(
                "order.created",
                "order-service.order.created",
                message(),
                "not recoverable"
        );

        assertEquals("message-1", record.messageId());
        assertEquals("order.created", record.eventName());
        assertEquals("order-service.order.created", record.consumer());
        assertEquals("rabbit", record.transport());
        assertEquals("{\"orderId\":\"order-1\"}", record.payload());
        assertEquals(4, record.retryMetadata().retryCount());
        assertEquals("discarded", record.error().errorType());
        assertEquals("not recoverable", record.error().message());
        assertEquals(Instant.parse("2026-05-18T00:00:00Z"), record.deadLetteredAt());
    }

    private static RabbitReliableMessageProperties properties() {
        RabbitReliableMessageProperties properties = new RabbitReliableMessageProperties();
        properties.setServiceName("order-service");
        properties.getRetry().setAttempts(5);
        return properties;
    }

    private static Clock clock() {
        return Clock.fixed(Instant.parse("2026-05-18T00:00:00Z"), ZoneOffset.UTC);
    }

    private static Message message() {
        MessageProperties properties = new MessageProperties();
        properties.setHeader(ReliableMessageHeaders.MESSAGE_ID, "message-1");
        properties.setHeader(ReliableMessageHeaders.RETRY_COUNT, 4);
        return new Message("{\"orderId\":\"order-1\"}".getBytes(java.nio.charset.StandardCharsets.UTF_8), properties);
    }
}
