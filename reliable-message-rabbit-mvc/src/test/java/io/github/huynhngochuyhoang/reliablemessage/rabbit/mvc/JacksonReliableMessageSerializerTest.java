package io.github.huynhngochuyhoang.reliablemessage.rabbit.mvc;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessage;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class JacksonReliableMessageSerializerTest {

    @Test
    void roundTripsReliableMessageEnvelope() {
        JacksonReliableMessageSerializer serializer = new JacksonReliableMessageSerializer(new ObjectMapper());
        ReliableMessage<OrderCreated> message = new ReliableMessage<>(
                "message-1",
                "order.created",
                "order-1",
                "event-1",
                "correlation-1",
                "trace-1",
                Instant.parse("2026-05-17T00:00:00Z"),
                Map.of("source", "orders"),
                new OrderCreated("order-1")
        );

        byte[] content = serializer.serialize(message);
        ReliableMessage<OrderCreated> deserialized = serializer.deserialize(content, OrderCreated.class);

        assertEquals(message, deserialized);
    }

    record OrderCreated(String orderId) {
    }
}
