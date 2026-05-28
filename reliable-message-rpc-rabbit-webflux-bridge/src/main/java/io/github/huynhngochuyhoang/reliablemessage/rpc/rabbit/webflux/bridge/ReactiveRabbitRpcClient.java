package io.github.huynhngochuyhoang.reliablemessage.rpc.rabbit.webflux.bridge;

import org.springframework.core.ParameterizedTypeReference;
import reactor.core.publisher.Mono;

public interface ReactiveRabbitRpcClient {

    <T> Mono<T> request(String route, Object request, Class<T> responseType);

    default <T> Mono<T> request(String route, Object request, Class<T> responseType, RpcOptions options) {
        return request(route, request, ParameterizedTypeReference.forType(responseType), options);
    }

    <T> Mono<T> request(
            String route,
            Object request,
            ParameterizedTypeReference<T> responseType,
            RpcOptions options
    );
}
