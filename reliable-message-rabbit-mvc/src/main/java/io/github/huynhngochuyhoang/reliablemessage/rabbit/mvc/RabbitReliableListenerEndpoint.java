package io.github.huynhngochuyhoang.reliablemessage.rabbit.mvc;

import java.lang.reflect.Method;

public record RabbitReliableListenerEndpoint(
        String beanName,
        Object bean,
        Method method,
        String eventName,
        String queueName,
        Class<?> payloadType
) {
}
