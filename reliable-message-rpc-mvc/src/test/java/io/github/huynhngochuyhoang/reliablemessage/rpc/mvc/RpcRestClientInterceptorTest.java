package io.github.huynhngochuyhoang.reliablemessage.rpc.mvc;

import io.github.huynhngochuyhoang.reliablemessage.rpc.RpcExceptionClassifier;
import io.github.huynhngochuyhoang.reliablemessage.rpc.RpcMetrics;
import io.github.huynhngochuyhoang.reliablemessage.rpc.RpcRetryPolicy;
import io.github.huynhngochuyhoang.reliablemessage.rpc.RpcTimeoutPolicy;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.http.*;
import org.springframework.http.client.ClientHttpRequestExecution;
import org.springframework.http.client.ClientHttpResponse;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RpcRestClientInterceptorTest {

    @Test
    void retriesTimeoutClassifiedFailuresWhenPolicyAllowsIt() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        RpcRestClientInterceptor interceptor = interceptor(
                new RpcRetryPolicy(2, List.of(Duration.ZERO)),
                RpcTimeoutPolicy.none()
        );
        ClientHttpRequestExecution execution = (request, body) -> {
            if (attempts.incrementAndGet() == 1) {
                throw new SocketTimeoutException("read timed out");
            }
            return new TestResponse(HttpStatus.OK);
        };

        ClientHttpResponse response = interceptor.intercept(new TestRequest(), new byte[0], execution);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(attempts).hasValue(2);
    }

    @Test
    void returnsPromptlyWhenTimeoutWrapperExecutionHangs() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        RpcRestClientInterceptor interceptor = interceptor(
                new RpcRetryPolicy(1, List.of(Duration.ZERO)),
                new RpcTimeoutPolicy(Duration.ofMillis(20)),
                command -> new Thread(command, "rpc-timeout-test").start()
        );
        ClientHttpRequestExecution execution = (request, body) -> {
            started.countDown();
            try {
                release.await();
                return new TestResponse(HttpStatus.OK);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IOException(interrupted);
            }
        };

        long startedAt = System.nanoTime();
        try {
            assertThatThrownBy(() -> interceptor.intercept(new TestRequest(), new byte[0], execution))
                    .isInstanceOf(RuntimeException.class)
                    .hasRootCauseInstanceOf(java.util.concurrent.TimeoutException.class);
            assertThat(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt)).isLessThan(1000);
            assertThat(started.await(1, TimeUnit.SECONDS)).isTrue();
        } finally {
            release.countDown();
        }
    }

    @Test
    void usesConfiguredExecutorForBlockingTimeoutWrapper() throws Exception {
        AtomicReference<String> threadName = new AtomicReference<>();
        Executor executor = command -> new Thread(command, "rpc-test-executor").start();
        RpcRestClientInterceptor interceptor = interceptor(
                new RpcRetryPolicy(1, List.of(Duration.ZERO)),
                new RpcTimeoutPolicy(Duration.ofSeconds(1)),
                executor
        );
        ClientHttpRequestExecution execution = (request, body) -> {
            threadName.set(Thread.currentThread().getName());
            return new TestResponse(HttpStatus.OK);
        };

        ClientHttpResponse response = interceptor.intercept(new TestRequest(), new byte[0], execution);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(threadName).hasValue("rpc-test-executor");
    }

    private static RpcRestClientInterceptor interceptor(RpcRetryPolicy retryPolicy, RpcTimeoutPolicy timeoutPolicy) {
        return interceptor(retryPolicy, timeoutPolicy, Runnable::run);
    }

    private static RpcRestClientInterceptor interceptor(RpcRetryPolicy retryPolicy, RpcTimeoutPolicy timeoutPolicy, Executor executor) {
        return new RpcRestClientInterceptor(
                new RpcMetrics(new SimpleMeterRegistry(), "test_rpc"),
                RpcExceptionClassifier.defaults(),
                retryPolicy,
                timeoutPolicy,
                executor
        );
    }

    private static final class TestRequest implements HttpRequest {
        private final HttpHeaders headers = new HttpHeaders();
        private final Map<String, Object> attributes = new HashMap<>();

        @Override
        public HttpMethod getMethod() {
            return HttpMethod.GET;
        }

        @Override
        public URI getURI() {
            return URI.create("https://example.test/orders");
        }

        @Override
        public HttpHeaders getHeaders() {
            return headers;
        }

        @Override
        public Map<String, Object> getAttributes() {
            return attributes;
        }
    }

    private static final class TestResponse implements ClientHttpResponse {
        private final HttpStatus status;

        private TestResponse(HttpStatus status) {
            this.status = status;
        }

        @Override
        public HttpStatusCode getStatusCode() {
            return status;
        }

        @Override
        public String getStatusText() {
            return status.getReasonPhrase();
        }

        @Override
        public void close() {
        }

        @Override
        public InputStream getBody() {
            return new ByteArrayInputStream(new byte[0]);
        }

        @Override
        public HttpHeaders getHeaders() {
            return new HttpHeaders();
        }
    }
}
