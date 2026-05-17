package io.github.huynhngochuyhoang.reliablemessage.rabbit.mvc;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessage;
import io.github.huynhngochuyhoang.reliablemessage.core.serialization.MessageSerializer;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;

public class JacksonReliableMessageSerializer implements MessageSerializer {

    private static final TypeReference<Map<String, String>> STRING_MAP = new TypeReference<>() {
    };

    private final ObjectMapper objectMapper;

    public JacksonReliableMessageSerializer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper.copy()
                .findAndRegisterModules()
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    @Override
    public <T> byte[] serialize(ReliableMessage<T> message) {
        try {
            return objectMapper.writeValueAsBytes(message);
        } catch (IOException error) {
            throw new ReliableMessageSerializationException("Failed to serialize reliable message", error);
        }
    }

    @Override
    public <T> ReliableMessage<T> deserialize(byte[] content, Class<T> payloadType) {
        try {
            JsonNode root = objectMapper.readTree(content);
            T payload = objectMapper.treeToValue(root.get("payload"), payloadType);
            Map<String, String> headers = objectMapper.convertValue(root.get("headers"), STRING_MAP);

            return new ReliableMessage<>(
                    text(root, "messageId"),
                    text(root, "eventName"),
                    nullableText(root, "aggregateId"),
                    nullableText(root, "idempotencyKey"),
                    nullableText(root, "correlationId"),
                    nullableText(root, "traceId"),
                    Instant.parse(text(root, "occurredAt")),
                    headers,
                    payload
            );
        } catch (IOException | IllegalArgumentException error) {
            throw new ReliableMessageSerializationException("Failed to deserialize reliable message", error);
        }
    }

    private static String text(JsonNode root, String field) {
        JsonNode node = root.get(field);
        if (node == null || node.isNull()) {
            throw new IllegalArgumentException(field + " is required");
        }
        return node.asText();
    }

    private static String nullableText(JsonNode root, String field) {
        JsonNode node = root.get(field);
        return node == null || node.isNull() ? null : node.asText();
    }
}
