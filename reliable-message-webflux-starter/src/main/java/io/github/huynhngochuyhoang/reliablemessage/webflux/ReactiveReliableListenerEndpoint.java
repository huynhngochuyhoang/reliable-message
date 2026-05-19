package io.github.huynhngochuyhoang.reliablemessage.webflux;

import java.lang.reflect.Method;
import java.util.Objects;

public record ReactiveReliableListenerEndpoint(
        String beanName,
        Object bean,
        Method method,
        String eventName,
        Class<?> payloadType
) {

    public ReactiveReliableListenerEndpoint {
        beanName = requireText(beanName, "beanName");
        bean = Objects.requireNonNull(bean, "bean must not be null");
        method = Objects.requireNonNull(method, "method must not be null");
        eventName = requireText(eventName, "eventName");
        payloadType = Objects.requireNonNull(payloadType, "payloadType must not be null");
    }

    private static String requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }
}
