package io.github.huynhngochuyhoang.reliablemessage.rabbit.webflux.bridge;

import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessage;
import org.springframework.amqp.core.Message;

@FunctionalInterface
public interface ReactiveRabbitBridgeFailureHandler {

    void handleFailure(
            ReactiveRabbitBridgeListenerEndpoint endpoint,
            ReliableMessage<?> reliableMessage,
            Message amqpMessage,
            Throwable error
    );

    static ReactiveRabbitBridgeFailureHandler noop() {
        return (endpoint, reliableMessage, amqpMessage, error) -> {
        };
    }
}
