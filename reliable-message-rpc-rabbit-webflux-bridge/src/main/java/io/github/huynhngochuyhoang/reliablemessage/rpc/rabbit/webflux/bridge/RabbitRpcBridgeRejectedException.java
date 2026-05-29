package io.github.huynhngochuyhoang.reliablemessage.rpc.rabbit.webflux.bridge;

public class RabbitRpcBridgeRejectedException extends RuntimeException {

    public RabbitRpcBridgeRejectedException(String message) {
        super(message);
    }

    public RabbitRpcBridgeRejectedException(String message, Throwable cause) {
        super(message, cause);
    }
}
