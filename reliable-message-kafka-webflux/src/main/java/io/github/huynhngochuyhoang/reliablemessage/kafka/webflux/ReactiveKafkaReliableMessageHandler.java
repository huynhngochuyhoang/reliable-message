package io.github.huynhngochuyhoang.reliablemessage.kafka.webflux;

import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessage;
import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessageHeaders;
import io.github.huynhngochuyhoang.reliablemessage.core.serialization.MessageSerializer;
import io.github.huynhngochuyhoang.reliablemessage.webflux.IdempotencyStartResult;
import io.github.huynhngochuyhoang.reliablemessage.webflux.IdempotencyState;
import io.github.huynhngochuyhoang.reliablemessage.webflux.ReactiveIdempotencyStore;
import io.github.huynhngochuyhoang.reliablemessage.webflux.ReliableMessageReactorContext;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class ReactiveKafkaReliableMessageHandler {

    private final MessageSerializer serializer;
    private final ReactiveIdempotencyStore idempotencyStore;
    private final Duration idempotencyTtl;
    private final ReactiveKafkaRetryStrategy retryStrategy;

    public ReactiveKafkaReliableMessageHandler(
            MessageSerializer serializer,
            ReactiveIdempotencyStore idempotencyStore,
            Duration idempotencyTtl,
            ReactiveKafkaRetryStrategy retryStrategy
    ) {
        this.serializer = serializer;
        this.idempotencyStore = idempotencyStore;
        this.idempotencyTtl = idempotencyTtl == null ? Duration.ofHours(24) : idempotencyTtl;
        this.retryStrategy = retryStrategy;
    }

    public Mono<Void> handle(ReactiveKafkaReceivedRecord record, ReactiveKafkaReliableListenerEndpoint endpoint) {
        AtomicBoolean idempotencyStarted = new AtomicBoolean(false);
        AtomicReference<ReliableMessage<?>> messageRef = new AtomicReference<>();

        return waitUntilRetryDue(record)
                .then(Mono.fromCallable(() -> serializer.deserialize(record.value(), endpoint.payloadType())))
                .doOnNext(messageRef::set)
                .flatMap(message -> ReliableMessageReactorContext.writeMessage(
                        processMessage(record, endpoint, message, idempotencyStarted),
                        message
                ))
                .onErrorResume(error -> {
                    ReliableMessage<?> message = messageRef.get();
                    Mono<Void> markFailed = idempotencyStarted.get() && message != null
                            ? idempotencyStore.markFailed(message.idempotencyKey(), error)
                            : Mono.empty();
                    return markFailed.onErrorResume(markFailedError -> Mono.empty())
                            .then(routeFailure(record, endpoint, error))
                            .then(record.receiverOffset().commit())
                            .then(Mono.error(error));
                });
    }

    private Mono<Void> processMessage(
            ReactiveKafkaReceivedRecord record,
            ReactiveKafkaReliableListenerEndpoint endpoint,
            ReliableMessage<?> message,
            AtomicBoolean idempotencyStarted
    ) {
        ReactiveKafkaReliableListenerMethodInvoker invoker = new ReactiveKafkaReliableListenerMethodInvoker(endpoint.bean(), endpoint.method());
        return tryStart(message)
                .flatMap(startResult -> {
                    if (!startResult.started()) {
                        return startResult.state() == IdempotencyState.SUCCESS
                                ? record.receiverOffset().commit()
                                : Mono.empty();
                    }
                    idempotencyStarted.set(isIdempotencyEnabled(message));
                    return invoker.invoke(message)
                            .then(markSuccess(message))
                            .then(record.receiverOffset().commit());
                });
    }

    private Mono<IdempotencyStartResult> tryStart(ReliableMessage<?> message) {
        if (!isIdempotencyEnabled(message)) {
            return Mono.just(IdempotencyStartResult.startAccepted());
        }
        return idempotencyStore.tryStart(message.idempotencyKey(), idempotencyTtl);
    }

    private Mono<Void> markSuccess(ReliableMessage<?> message) {
        if (!isIdempotencyEnabled(message)) {
            return Mono.empty();
        }
        return idempotencyStore.markSuccess(message.idempotencyKey());
    }

    private boolean isIdempotencyEnabled(ReliableMessage<?> message) {
        return idempotencyStore != null
                && message.idempotencyKey() != null
                && !message.idempotencyKey().isBlank();
    }

    private Mono<Void> routeFailure(
            ReactiveKafkaReceivedRecord record,
            ReactiveKafkaReliableListenerEndpoint endpoint,
            Throwable error
    ) {
        if (retryStrategy == null) {
            return Mono.error(error);
        }
        return retryStrategy.routeFailure(record, endpoint, error);
    }

    private Mono<Void> waitUntilRetryDue(ReactiveKafkaReceivedRecord record) {
        String retryNotBefore = ReactiveKafkaRecordHeaders.value(record.headers(), ReliableMessageHeaders.RETRY_NOT_BEFORE);
        if (retryNotBefore == null || retryNotBefore.isBlank()) {
            return Mono.empty();
        }
        long retryNotBeforeEpochMillis;
        try {
            retryNotBeforeEpochMillis = Long.parseLong(retryNotBefore);
        } catch (NumberFormatException ex) {
            return Mono.empty();
        }
        long delayMillis = retryNotBeforeEpochMillis - Instant.now().toEpochMilli();
        if (delayMillis <= 0) {
            return Mono.empty();
        }
        return Mono.delay(Duration.ofMillis(delayMillis)).then();
    }
}
