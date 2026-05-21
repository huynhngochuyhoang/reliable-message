package io.github.huynhngochuyhoang.reliablemessage.kafka.webflux;

import reactor.core.publisher.Mono;

public interface ReactiveKafkaReceiverOffset {

    Mono<Void> commit();
}
