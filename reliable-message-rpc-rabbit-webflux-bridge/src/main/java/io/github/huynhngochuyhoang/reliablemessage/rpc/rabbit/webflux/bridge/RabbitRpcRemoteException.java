package io.github.huynhngochuyhoang.reliablemessage.rpc.rabbit.webflux.bridge;

public class RabbitRpcRemoteException extends RuntimeException {

    private final String errorCode;
    private final String errorMessage;
    private final String errorType;

    public RabbitRpcRemoteException(String errorCode, String errorMessage, String errorType) {
        super(errorMessage == null || errorMessage.isBlank() ? "Remote RPC error" : errorMessage);
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
        this.errorType = errorType;
    }

    public String getErrorCode() {
        return errorCode;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public String getErrorType() {
        return errorType;
    }
}
