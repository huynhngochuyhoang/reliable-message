package io.github.huynhngochuyhoang.reliablemessage.webflux;

import io.github.huynhngochuyhoang.reliablemessage.core.PublishOptions;
import reactor.core.publisher.Mono;

public interface ReactiveReliablePublisher {

    Mono<Void> publish(String eventName, Object payload, PublishOptions options);
}
