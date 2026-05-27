package io.github.huynhngochuyhoang.reliablemessage.rpc.rabbit.webflux.bridge;

import reactor.core.publisher.Mono;

public interface ReactiveRabbitRpcClient {

    <T> Mono<T> request(String route, Object request, Class<T> responseType);
}
