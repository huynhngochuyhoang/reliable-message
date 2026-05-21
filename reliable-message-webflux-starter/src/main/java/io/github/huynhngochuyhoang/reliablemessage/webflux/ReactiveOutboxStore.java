package io.github.huynhngochuyhoang.reliablemessage.webflux;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;

public interface ReactiveOutboxStore {

    Mono<Void> save(OutboxMessage message);

    Flux<OutboxMessage> findPending(int limit);

    Mono<Void> markPublished(String id);

    Mono<Void> markFailed(String id, Throwable error, Instant nextRetryAt);
}
