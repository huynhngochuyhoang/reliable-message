package io.github.huynhngochuyhoang.reliablemessage.rabbit.webflux.bridge;

import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessage;
import reactor.core.publisher.Mono;

import java.lang.reflect.InvocationTargetException;

public class ReactiveRabbitBridgeListenerMethodInvoker {

    public Mono<Void> invoke(ReactiveRabbitBridgeListenerEndpoint endpoint, ReliableMessage<?> message) {
        try {
            endpoint.method().setAccessible(true);
            Object result = endpoint.method().invoke(endpoint.bean(), message);
            if (result == null) {
                return Mono.error(new IllegalStateException("@ReactiveReliableListener returned null Mono: " + endpoint.method()));
            }
            return ((Mono<?>) result).then();
        } catch (IllegalAccessException error) {
            return Mono.error(error);
        } catch (InvocationTargetException error) {
            return Mono.error(error.getTargetException());
        }
    }
}
