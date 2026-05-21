package io.github.huynhngochuyhoang.reliablemessage.rpc.webflux;

import io.github.huynhngochuyhoang.reliablemessage.rpc.RpcContext;
import io.github.huynhngochuyhoang.reliablemessage.rpc.RpcExceptionClassifier;
import io.github.huynhngochuyhoang.reliablemessage.rpc.RpcHeaders;
import io.github.huynhngochuyhoang.reliablemessage.rpc.RpcMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.web.reactive.function.client.ClientRequest;
import org.springframework.web.reactive.function.client.ClientResponse;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.net.URI;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class RpcWebClientExchangeFilterTest {

    @Test
    void propagatesRpcHeadersFromReactorContext() {
        AtomicReference<ClientRequest> capturedRequest = new AtomicReference<>();
        RpcWebClientExchangeFilter filter = new RpcWebClientExchangeFilter(
                new RpcMetrics(new SimpleMeterRegistry(), "rpc_reactive"),
                RpcExceptionClassifier.defaults()
        );
        ClientRequest request = ClientRequest.create(org.springframework.http.HttpMethod.GET, URI.create("https://example.test"))
                .build();

        Mono<ClientResponse> result = filter.filter(request, outbound -> {
            capturedRequest.set(outbound);
            return Mono.just(ClientResponse.create(HttpStatus.OK).build());
        }).contextWrite(ReactiveRpcContext.write(RpcContext.builder()
                .correlationId("correlation-1")
                .requestId("request-1")
                .traceId("trace-1")
                .tenantId("tenant-1")
                .build()));

        StepVerifier.create(result)
                .expectNextCount(1)
                .verifyComplete();

        assertThat(capturedRequest.get().headers().getFirst(RpcHeaders.CORRELATION_ID)).isEqualTo("correlation-1");
        assertThat(capturedRequest.get().headers().getFirst(RpcHeaders.REQUEST_ID)).isEqualTo("request-1");
        assertThat(capturedRequest.get().headers().getFirst(RpcHeaders.TRACE_ID)).isEqualTo("trace-1");
        assertThat(capturedRequest.get().headers().getFirst(RpcHeaders.TENANT_ID)).isEqualTo("tenant-1");
    }
}
