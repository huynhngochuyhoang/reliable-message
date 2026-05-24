package io.github.huynhngochuyhoang.reliablemessage.rabbit.webflux.bridge;

import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessage;
import org.springframework.amqp.core.Message;

@FunctionalInterface
public interface ReactiveRabbitBridgeFailureOutcomeHandler extends ReactiveRabbitBridgeFailureHandler {

    ReactiveRabbitBridgeFailureOutcome handleFailureWithOutcome(
            ReactiveRabbitBridgeListenerEndpoint endpoint,
            ReliableMessage<?> reliableMessage,
            Message amqpMessage,
            Throwable error
    );

    @Override
    default void handleFailure(
            ReactiveRabbitBridgeListenerEndpoint endpoint,
            ReliableMessage<?> reliableMessage,
            Message amqpMessage,
            Throwable error
    ) {
        handleFailureWithOutcome(endpoint, reliableMessage, amqpMessage, error);
    }
}
