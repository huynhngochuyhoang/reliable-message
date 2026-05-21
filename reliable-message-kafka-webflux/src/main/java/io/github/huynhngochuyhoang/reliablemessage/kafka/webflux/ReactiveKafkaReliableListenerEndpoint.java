package io.github.huynhngochuyhoang.reliablemessage.kafka.webflux;

import java.lang.reflect.Method;

public record ReactiveKafkaReliableListenerEndpoint(
        String beanName,
        Object bean,
        Method method,
        String eventName,
        String topicName,
        String consumerGroup,
        Class<?> payloadType
) {
}
