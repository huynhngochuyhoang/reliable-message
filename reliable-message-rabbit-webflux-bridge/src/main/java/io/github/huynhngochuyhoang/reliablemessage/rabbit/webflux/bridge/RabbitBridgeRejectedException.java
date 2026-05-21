package io.github.huynhngochuyhoang.reliablemessage.rabbit.webflux.bridge;

import java.util.concurrent.RejectedExecutionException;

public class RabbitBridgeRejectedException extends RejectedExecutionException {

    public RabbitBridgeRejectedException(String message) {
        super(message);
    }

    public RabbitBridgeRejectedException(String message, Throwable cause) {
        super(message, cause);
    }
}
