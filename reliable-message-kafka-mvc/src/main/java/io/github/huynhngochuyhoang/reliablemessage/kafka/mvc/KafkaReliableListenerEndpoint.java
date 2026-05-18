package io.github.huynhngochuyhoang.reliablemessage.kafka.mvc;

import java.lang.reflect.Method;

public record KafkaReliableListenerEndpoint(
        String beanName,
        Object bean,
        Method method,
        String eventName,
        String topicName,
        String consumerGroup,
        Class<?> payloadType
) {
}
