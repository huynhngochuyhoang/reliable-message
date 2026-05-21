package io.github.huynhngochuyhoang.reliablemessage.kafka.webflux;

import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessage;
import org.springframework.util.ReflectionUtils;
import reactor.core.publisher.Mono;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class ReactiveKafkaReliableListenerMethodInvoker {

    private final Object bean;
    private final Method method;

    public ReactiveKafkaReliableListenerMethodInvoker(Object bean, Method method) {
        this.bean = bean;
        this.method = method;
        ReflectionUtils.makeAccessible(method);
    }

    public Mono<Void> invoke(ReliableMessage<?> message) {
        try {
            Object result = method.invoke(bean, message);
            if (result == null) {
                return Mono.error(new IllegalStateException("@ReactiveReliableListener returned null Mono: " + method));
            }
            return ((Mono<?>) result).then();
        } catch (IllegalAccessException error) {
            return Mono.error(error);
        } catch (InvocationTargetException error) {
            return Mono.error(error.getTargetException());
        }
    }
}
