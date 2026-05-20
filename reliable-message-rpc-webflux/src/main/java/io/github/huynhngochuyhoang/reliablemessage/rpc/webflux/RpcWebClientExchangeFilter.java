package io.github.huynhngochuyhoang.reliablemessage.rpc.webflux;

import io.github.huynhngochuyhoang.reliablemessage.rpc.RpcExceptionClassifier;
import io.github.huynhngochuyhoang.reliablemessage.rpc.RpcHeaders;
import io.github.huynhngochuyhoang.reliablemessage.rpc.RpcMetrics;
import io.micrometer.core.instrument.Timer;
import org.springframework.web.reactive.function.client.ClientRequest;
import org.springframework.web.reactive.function.client.ExchangeFilterFunction;
import org.springframework.web.reactive.function.client.ExchangeFunction;
import org.springframework.web.reactive.function.client.ClientResponse;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.Objects;

public class RpcWebClientExchangeFilter implements ExchangeFilterFunction {

    private final RpcMetrics metrics;
    private final RpcExceptionClassifier exceptionClassifier;

    public RpcWebClientExchangeFilter(RpcMetrics metrics, RpcExceptionClassifier exceptionClassifier) {
        this.metrics = Objects.requireNonNull(metrics, "metrics must not be null");
        this.exceptionClassifier = Objects.requireNonNull(exceptionClassifier, "exceptionClassifier must not be null");
    }

    @Override
    public Mono<ClientResponse> filter(ClientRequest request, ExchangeFunction next) {
        return Mono.deferContextual(context -> {
            ClientRequest outbound = ReactiveRpcContext.current(context)
                    .map(RpcHeaders::from)
                    .map(headers -> withHeaders(request, headers))
                    .orElse(request);

            Timer.Sample sample = metrics.start();
            return next.exchange(outbound)
                    .doOnNext(response -> {
                        String status = Integer.toString(response.statusCode().value());
                        metrics.request("webflux", "http", status);
                        metrics.duration(sample, "webflux", "http", status);
                    })
                    .doOnError(error -> {
                        if (exceptionClassifier.timeout(error)) {
                            metrics.timeout("webflux", "http");
                        }
                        metrics.failure("webflux", "http", error.getClass().getSimpleName());
                        metrics.duration(sample, "webflux", "http", "failed");
                    });
        });
    }

    private static ClientRequest withHeaders(ClientRequest request, Map<String, String> headers) {
        return ClientRequest.from(request)
                .headers(httpHeaders -> headers.forEach((name, value) -> {
                    if (!httpHeaders.containsKey(name)) {
                        httpHeaders.set(name, value);
                    }
                }))
                .build();
    }
}
