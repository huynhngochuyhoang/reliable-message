package io.github.huynhngochuyhoang.reliablemessage.rpc.mvc;

import io.github.huynhngochuyhoang.reliablemessage.rpc.*;
import io.micrometer.core.instrument.Timer;
import org.springframework.http.HttpRequest;
import org.springframework.http.client.ClientHttpRequestExecution;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.http.client.ClientHttpResponse;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class RpcRestClientInterceptor implements ClientHttpRequestInterceptor {

    private final RpcMetrics metrics;
    private final RpcExceptionClassifier exceptionClassifier;
    private final RpcRetryPolicy retryPolicy;
    private final RpcTimeoutPolicy timeoutPolicy;

    public RpcRestClientInterceptor(
            RpcMetrics metrics,
            RpcExceptionClassifier exceptionClassifier,
            RpcRetryPolicy retryPolicy,
            RpcTimeoutPolicy timeoutPolicy
    ) {
        this.metrics = Objects.requireNonNull(metrics, "metrics must not be null");
        this.exceptionClassifier = Objects.requireNonNull(exceptionClassifier, "exceptionClassifier must not be null");
        this.retryPolicy = Objects.requireNonNull(retryPolicy, "retryPolicy must not be null");
        this.timeoutPolicy = Objects.requireNonNull(timeoutPolicy, "timeoutPolicy must not be null");
    }

    @Override
    public ClientHttpResponse intercept(HttpRequest request, byte[] body, ClientHttpRequestExecution execution) throws IOException {
        RpcContextHolder.current()
                .map(RpcHeaders::from)
                .ifPresent(headers -> apply(headers, request));

        Timer.Sample sample = metrics.start();
        int attempts = Math.max(1, retryPolicy.maxAttempts());
        for (int attempt = 1; attempt <= attempts; attempt++) {
            try {
                ClientHttpResponse response = executeWithTimeout(request, body, execution);
                String status = Integer.toString(response.getStatusCode().value());
                metrics.request("mvc", "http", status);
                metrics.duration(sample, "mvc", "http", status);
                return response;
            } catch (IOException | RuntimeException error) {
                if (exceptionClassifier.timeout(error)) {
                    metrics.timeout("mvc", "http");
                }
                if (attempt >= attempts || !exceptionClassifier.retryable(error)) {
                    metrics.failure("mvc", "http", error.getClass().getSimpleName());
                    metrics.duration(sample, "mvc", "http", "failed");
                    throw error;
                }
                metrics.retry("mvc", "http");
                sleepBeforeRetry(attempt);
            }
        }
        throw new IllegalStateException("unreachable");
    }

    private ClientHttpResponse executeWithTimeout(HttpRequest request, byte[] body, ClientHttpRequestExecution execution) throws IOException {
        Duration timeout = timeoutPolicy.requestTimeout();
        if (!timeoutPolicy.enabled()) {
            return execution.execute(request, body);
        }
        CompletableFuture<ClientHttpResponse> future = CompletableFuture.supplyAsync(() -> {
            try {
                return execution.execute(request, body);
            } catch (IOException ioException) {
                throw new RuntimeException(ioException);
            }
        });
        try {
            return future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException timeoutException) {
            waitForTimedOutAttempt(future);
            throw new RuntimeException(timeoutException);
        } catch (InterruptedException interruptedException) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for RPC response", interruptedException);
        } catch (ExecutionException executionException) {
            Throwable cause = executionException.getCause();
            if (cause instanceof RuntimeException runtime && runtime.getCause() instanceof IOException ioCause) {
                throw ioCause;
            }
            if (cause instanceof RuntimeException runtime) {
                throw runtime;
            }
            throw new IOException("RPC execution failed", cause);
        }
    }

    private void waitForTimedOutAttempt(CompletableFuture<ClientHttpResponse> future) throws IOException {
        try {
            ClientHttpResponse lateResponse = future.get();
            if (lateResponse != null) {
                lateResponse.close();
            }
        } catch (InterruptedException interruptedException) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for timed out RPC response", interruptedException);
        } catch (ExecutionException ignored) {
            // The caller already observes the timeout; this only waits until the attempt is no longer in flight.
        }
    }

    private void sleepBeforeRetry(int nextAttempt) {
        Duration delay = retryPolicy.delayForAttempt(nextAttempt);
        if (delay.isNegative() || delay.isZero()) {
            return;
        }
        try {
            Thread.sleep(delay.toMillis());
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while applying RPC retry backoff", interrupted);
        }
    }

    private static void apply(Map<String, String> headers, HttpRequest request) {
        headers.forEach((name, value) -> request.getHeaders().putIfAbsent(name, java.util.List.of(value)));
    }
}
