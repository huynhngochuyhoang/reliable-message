package io.github.huynhngochuyhoang.reliablemessage.core.serialization;

import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessage;

public interface MessageSerializer {

    <T> byte[] serialize(ReliableMessage<T> message);

    <T> ReliableMessage<T> deserialize(byte[] content, Class<T> payloadType);
}
