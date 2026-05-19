package io.github.huynhngochuyhoang.reliablemessage.kafka.webflux;

import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;

public class ReactiveKafkaReliableListenerContainer {

    private final KafkaReceiver<String, byte[]> kafkaReceiver;
    private final ReactiveKafkaReliableListenerEndpoint endpoint;
    private final ReactiveKafkaReliableMessageHandler handler;
    private final int maxConcurrency;
    private final int prefetch;
    private Disposable subscription;

    public ReactiveKafkaReliableListenerContainer(
            KafkaReceiver<String, byte[]> kafkaReceiver,
            ReactiveKafkaReliableListenerEndpoint endpoint,
            ReactiveKafkaReliableMessageHandler handler,
            int maxConcurrency,
            int prefetch
    ) {
        this.kafkaReceiver = kafkaReceiver;
        this.endpoint = endpoint;
        this.handler = handler;
        this.maxConcurrency = maxConcurrency;
        this.prefetch = prefetch;
    }

    public void start() {
        if (subscription != null && !subscription.isDisposed()) {
            return;
        }
        subscription = kafkaReceiver.receive()
                .limitRate(prefetch)
                .flatMap(record -> handler.handle(new ReactorKafkaReceivedRecord(record), endpoint)
                        .onErrorResume(error -> Mono.empty()), maxConcurrency, prefetch)
                .subscribe();
    }

    public void stop() {
        if (subscription != null) {
            subscription.dispose();
        }
    }

    public int maxConcurrency() {
        return maxConcurrency;
    }

    public int prefetch() {
        return prefetch;
    }
}
