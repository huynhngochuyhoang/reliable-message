package io.github.huynhngochuyhoang.reliablemessage.webflux;

import reactor.core.publisher.Mono;

import java.time.Duration;

public interface ReactiveIdempotencyStore {

    Mono<IdempotencyStartResult> tryStart(String key, Duration ttl);

    Mono<Void> markSuccess(String key);

    Mono<Void> markFailed(String key, Throwable error);
}
