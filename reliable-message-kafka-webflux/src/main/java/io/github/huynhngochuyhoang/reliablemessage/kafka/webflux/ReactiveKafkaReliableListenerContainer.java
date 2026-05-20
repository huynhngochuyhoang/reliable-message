package io.github.huynhngochuyhoang.reliablemessage.kafka.webflux;

import reactor.core.Disposable;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.concurrent.Semaphore;

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
        Semaphore concurrencyLimiter = new Semaphore(Math.max(1, maxConcurrency));
        subscription = kafkaReceiver.receive()
                .limitRate(prefetch)
                .groupBy(record -> record.receiverOffset().topicPartition())
                .flatMap(partitionRecords -> partitionRecords
                        .concatMap(record -> withConcurrencyLimit(
                                concurrencyLimiter,
                                handler.handle(new ReactorKafkaReceivedRecord(record), endpoint)
                        )))
                .retryWhen(Retry.backoff(Long.MAX_VALUE, Duration.ofSeconds(1))
                        .maxBackoff(Duration.ofSeconds(30)))
                .subscribe();
    }

    private static Mono<Void> withConcurrencyLimit(Semaphore limiter, Mono<Void> action) {
        return Mono.fromRunnable(limiter::acquireUninterruptibly)
                .subscribeOn(Schedulers.boundedElastic())
                .then(action)
                .doFinally(signal -> limiter.release());
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
