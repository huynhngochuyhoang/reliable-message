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
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

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
    void waitsForTimedOutAttemptBeforeRetrying() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        AtomicInteger active = new AtomicInteger();
        AtomicInteger maxActive = new AtomicInteger();
        RpcRestClientInterceptor interceptor = interceptor(
                new RpcRetryPolicy(2, List.of(Duration.ZERO)),
                new RpcTimeoutPolicy(Duration.ofMillis(20))
        );
        ClientHttpRequestExecution execution = (request, body) -> {
            int running = active.incrementAndGet();
            maxActive.accumulateAndGet(running, Math::max);
            try {
                if (attempts.incrementAndGet() == 1) {
                    Thread.sleep(80);
                }
                return new TestResponse(HttpStatus.OK);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IOException(interrupted);
            } finally {
                active.decrementAndGet();
            }
        };

        ClientHttpResponse response = interceptor.intercept(new TestRequest(), new byte[0], execution);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(attempts).hasValue(2);
        assertThat(maxActive).hasValue(1);
    }

    private static RpcRestClientInterceptor interceptor(RpcRetryPolicy retryPolicy, RpcTimeoutPolicy timeoutPolicy) {
        return new RpcRestClientInterceptor(
                new RpcMetrics(new SimpleMeterRegistry(), "test_rpc"),
                RpcExceptionClassifier.defaults(),
                retryPolicy,
                timeoutPolicy
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
