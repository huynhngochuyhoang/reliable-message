package io.github.huynhngochuyhoang.reliablemessage.kafka.webflux;

import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

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
                .limitRate(Math.max(1, prefetch))
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

    static Mono<Void> withConcurrencyLimit(Semaphore limiter, Mono<Void> action) {
        AtomicBoolean acquired = new AtomicBoolean(false);
        return Mono.fromCallable(() -> {
                    try {
                        limiter.acquire();
                        acquired.set(true);
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException("Interrupted while acquiring listener concurrency permit", interrupted);
                    }
                    return true;
                })
                .subscribeOn(Schedulers.boundedElastic())
                .then(action)
                .doFinally(signal -> {
                    if (acquired.get()) {
                        limiter.release();
                    }
                });
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
