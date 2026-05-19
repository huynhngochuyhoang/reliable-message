package io.github.huynhngochuyhoang.reliablemessage.kafka.mvc;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessage;
import io.github.huynhngochuyhoang.reliablemessage.core.serialization.MessageSerializer;

public class JacksonReliableMessageSerializer implements MessageSerializer {

    private final ObjectMapper objectMapper;

    public JacksonReliableMessageSerializer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper.copy().registerModule(new JavaTimeModule());
    }

    @Override
    public <T> byte[] serialize(ReliableMessage<T> message) {
        try {
            return objectMapper.writeValueAsBytes(message);
        } catch (Exception error) {
            throw new ReliableMessageSerializationException("Failed to serialize reliable message", error);
        }
    }

    @Override
    public <T> ReliableMessage<T> deserialize(byte[] content, Class<T> payloadType) {
        try {
            JavaType type = objectMapper.getTypeFactory().constructParametricType(ReliableMessage.class, payloadType);
            return objectMapper.readValue(content, type);
        } catch (Exception error) {
            throw new ReliableMessageSerializationException("Failed to deserialize reliable message", error);
        }
    }
}
