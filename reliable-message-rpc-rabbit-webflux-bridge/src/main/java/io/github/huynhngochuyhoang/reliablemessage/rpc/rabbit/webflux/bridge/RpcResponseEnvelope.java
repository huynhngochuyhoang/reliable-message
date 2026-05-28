package io.github.huynhngochuyhoang.reliablemessage.rpc.rabbit.webflux.bridge;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public final class RpcResponseEnvelope<T> {

    private RpcEnvelopeStatus status;
    private T payload;
    private String errorCode;
    private String errorMessage;
    private String errorType;
    private Map<String, String> headers = Collections.emptyMap();

    public RpcResponseEnvelope() {
    }

    private RpcResponseEnvelope(RpcEnvelopeStatus status, T payload, String errorCode, String errorMessage, String errorType) {
        this.status = status;
        this.payload = payload;
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
        this.errorType = errorType;
    }

    public static <T> RpcResponseEnvelope<T> success(T payload) {
        return new RpcResponseEnvelope<>(RpcEnvelopeStatus.SUCCESS, payload, null, null, null);
    }

    public static <T> RpcResponseEnvelope<T> error(String errorCode, String errorMessage, String errorType) {
        return new RpcResponseEnvelope<>(RpcEnvelopeStatus.ERROR, null, errorCode, errorMessage, errorType);
    }

    public RpcEnvelopeStatus getStatus() {
        return status;
    }

    public void setStatus(RpcEnvelopeStatus status) {
        this.status = status;
    }

    public T getPayload() {
        return payload;
    }

    public void setPayload(T payload) {
        this.payload = payload;
    }

    public String getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(String errorCode) {
        this.errorCode = errorCode;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public String getErrorType() {
        return errorType;
    }

    public void setErrorType(String errorType) {
        this.errorType = errorType;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public void setHeaders(Map<String, String> headers) {
        this.headers = headers == null ? Collections.emptyMap() : new LinkedHashMap<>(headers);
    }

    public enum RpcEnvelopeStatus {
        SUCCESS,
        ERROR
    }
}
