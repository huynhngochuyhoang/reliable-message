package io.github.huynhngochuyhoang.reliablemessage.core.serialization;

import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessage;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MessageSerializerTest {

    @Test
    void definesRoundTripContract() {
        MessageSerializer serializer = new TestMessageSerializer();
        ReliableMessage<String> message = new ReliableMessage<>(
                "message-1",
                "order.created",
                "order-1",
                "event-1",
                "correlation-1",
                "trace-1",
                Instant.parse("2026-05-17T00:00:00Z"),
                Map.of(),
                "payload"
        );

        byte[] content = serializer.serialize(message);
        ReliableMessage<String> deserialized = serializer.deserialize(content, String.class);

        assertEquals(message, deserialized);
    }

    private static final class TestMessageSerializer implements MessageSerializer {

        @Override
        public <T> byte[] serialize(ReliableMessage<T> message) {
            return message.payload().toString().getBytes(StandardCharsets.UTF_8);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> ReliableMessage<T> deserialize(byte[] content, Class<T> payloadType) {
            return new ReliableMessage<>(
                    "message-1",
                    "order.created",
                    "order-1",
                    "event-1",
                    "correlation-1",
                    "trace-1",
                    Instant.parse("2026-05-17T00:00:00Z"),
                    Map.of(),
                    (T) new String(content, StandardCharsets.UTF_8)
            );
        }
    }
}
