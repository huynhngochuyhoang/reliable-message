package io.github.huynhngochuyhoang.reliablemessage.rpc.mvc;

import io.github.huynhngochuyhoang.reliablemessage.rpc.RpcContextHolder;
import io.github.huynhngochuyhoang.reliablemessage.rpc.RpcExceptionClassifier;
import io.github.huynhngochuyhoang.reliablemessage.rpc.RpcHeaders;
import io.github.huynhngochuyhoang.reliablemessage.rpc.RpcMetrics;
import io.micrometer.core.instrument.Timer;
import org.springframework.http.HttpRequest;
import org.springframework.http.client.ClientHttpRequestExecution;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.http.client.ClientHttpResponse;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class RpcRestClientInterceptor implements ClientHttpRequestInterceptor {

    private final RpcMetrics metrics;
    private final RpcExceptionClassifier exceptionClassifier;

    public RpcRestClientInterceptor(RpcMetrics metrics, RpcExceptionClassifier exceptionClassifier) {
        this.metrics = Objects.requireNonNull(metrics, "metrics must not be null");
        this.exceptionClassifier = Objects.requireNonNull(exceptionClassifier, "exceptionClassifier must not be null");
    }

    @Override
    public ClientHttpResponse intercept(HttpRequest request, byte[] body, ClientHttpRequestExecution execution) throws IOException {
        RpcContextHolder.current()
                .map(RpcHeaders::from)
                .ifPresent(headers -> apply(headers, request));

        Timer.Sample sample = metrics.start();
        try {
            ClientHttpResponse response = execution.execute(request, body);
            String status = Integer.toString(response.getStatusCode().value());
            metrics.request("mvc", "http", status);
            metrics.duration(sample, "mvc", "http", status);
            return response;
        } catch (IOException | RuntimeException error) {
            if (exceptionClassifier.timeout(error)) {
                metrics.timeout("mvc", "http");
            }
            metrics.failure("mvc", "http", error.getClass().getSimpleName());
            metrics.duration(sample, "mvc", "http", "failed");
            throw error;
        }
    }

    private static void apply(Map<String, String> headers, HttpRequest request) {
        headers.forEach((name, value) -> request.getHeaders().putIfAbsent(name, java.util.List.of(value)));
    }
}
