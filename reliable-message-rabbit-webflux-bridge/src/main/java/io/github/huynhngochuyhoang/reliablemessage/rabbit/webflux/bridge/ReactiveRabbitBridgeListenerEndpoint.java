package io.github.huynhngochuyhoang.reliablemessage.rabbit.webflux.bridge;

import java.lang.reflect.Method;
import java.util.Objects;

public record ReactiveRabbitBridgeListenerEndpoint(
        String beanName,
        Object bean,
        Method method,
        String eventName,
        String queueName,
        Class<?> payloadType
) {

    public ReactiveRabbitBridgeListenerEndpoint {
        beanName = requireText(beanName, "beanName");
        bean = Objects.requireNonNull(bean, "bean must not be null");
        method = Objects.requireNonNull(method, "method must not be null");
        eventName = requireText(eventName, "eventName");
        queueName = requireText(queueName, "queueName");
        payloadType = Objects.requireNonNull(payloadType, "payloadType must not be null");
    }

    private static String requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }
}
