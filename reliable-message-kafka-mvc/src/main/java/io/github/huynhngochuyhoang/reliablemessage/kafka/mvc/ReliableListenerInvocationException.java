package io.github.huynhngochuyhoang.reliablemessage.kafka.mvc;

public class ReliableListenerInvocationException extends RuntimeException {

    public ReliableListenerInvocationException(String message, Throwable cause) {
        super(message, cause);
    }
}
