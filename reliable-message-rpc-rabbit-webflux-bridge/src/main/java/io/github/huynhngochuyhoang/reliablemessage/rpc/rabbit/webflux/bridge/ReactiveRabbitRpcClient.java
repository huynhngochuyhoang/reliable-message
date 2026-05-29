package io.github.huynhngochuyhoang.reliablemessage.rpc.rabbit.webflux.bridge;

import org.springframework.core.ParameterizedTypeReference;
import reactor.core.publisher.Mono;

public interface ReactiveRabbitRpcClient {

    <T> Mono<T> request(String route, Object request, Class<T> responseType);

    default <T> Mono<T> request(String route, Object request, Class<T> responseType, RpcOptions options) {
        return request(route, request, responseType);
    }

    default <T> Mono<T> request(
            String route,
            Object request,
            ParameterizedTypeReference<T> responseType,
            RpcOptions options
    ) {
        if (responseType.getType() instanceof Class<?> type) {
            @SuppressWarnings("unchecked")
            Class<T> responseClass = (Class<T>) type;
            return request(route, request, responseClass);
        }
        return Mono.error(new UnsupportedOperationException("Parameterized RPC response types require a ReactiveRabbitRpcClient implementation that supports ParameterizedTypeReference"));
    }
}
