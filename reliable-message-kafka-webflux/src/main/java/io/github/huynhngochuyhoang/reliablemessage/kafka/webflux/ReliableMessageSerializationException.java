package io.github.huynhngochuyhoang.reliablemessage.kafka.webflux;

public class ReliableMessageSerializationException extends RuntimeException {

    public ReliableMessageSerializationException(String message, Throwable cause) {
        super(message, cause);
    }
}
