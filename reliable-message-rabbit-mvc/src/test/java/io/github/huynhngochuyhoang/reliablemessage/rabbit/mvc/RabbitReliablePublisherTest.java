package io.github.huynhngochuyhoang.reliablemessage.rabbit.mvc;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.huynhngochuyhoang.reliablemessage.core.PublishOptions;
import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessage;
import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessageHeaders;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.core.RabbitOperations;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;

class RabbitReliablePublisherTest {

    @Test
    void publishesEnvelopeWithRabbitHeaders() {
        RabbitTemplate rabbitTemplate = org.mockito.Mockito.mock(RabbitTemplate.class);
        when(rabbitTemplate.invoke(any(RabbitOperations.OperationsCallback.class))).thenAnswer(invocation -> {
            RabbitOperations.OperationsCallback<?> callback = invocation.getArgument(0);
            return callback.doInRabbit(rabbitTemplate);
        });
        RabbitReliableMessageProperties properties = new RabbitReliableMessageProperties();
        properties.getRabbit().setExchange("app.events");
        JacksonReliableMessageSerializer serializer = new JacksonReliableMessageSerializer(new ObjectMapper());
        RabbitReliablePublisher publisher = new RabbitReliablePublisher(
                rabbitTemplate,
                serializer,
                properties,
                Clock.fixed(Instant.parse("2026-05-17T00:00:00Z"), ZoneOffset.UTC),
                new SimpleMeterRegistry()
        );

        publisher.publish(
                "order.created",
                new OrderCreated("order-1"),
                PublishOptions.builder()
                        .aggregateId("order-1")
                        .idempotencyKey("event-1")
                        .correlationId("correlation-1")
                        .partitionKey("order-1")
                        .build()
        );

        ArgumentCaptor<Message> messageCaptor = ArgumentCaptor.forClass(Message.class);
        verify(rabbitTemplate).send(eq("app.events"), eq("order.created"), messageCaptor.capture());
        verify(rabbitTemplate).waitForConfirmsOrDie(5000L);

        Message message = messageCaptor.getValue();
        ReliableMessage<OrderCreated> envelope = serializer.deserialize(message.getBody(), OrderCreated.class);

        assertEquals("order.created", envelope.eventName());
        assertEquals("order-1", envelope.payload().orderId());
        assertEquals("correlation-1", envelope.correlationId());
        assertEquals("correlation-1", message.getMessageProperties().getHeaders().get(ReliableMessageHeaders.CORRELATION_ID));
        assertNotNull(message.getMessageProperties().getHeaders().get(ReliableMessageHeaders.MESSAGE_ID));
    }

    record OrderCreated(String orderId) {
    }
}
