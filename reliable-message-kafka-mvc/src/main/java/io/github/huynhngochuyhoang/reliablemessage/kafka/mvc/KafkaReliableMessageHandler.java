package io.github.huynhngochuyhoang.reliablemessage.kafka.mvc;

import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessage;
import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessageHeaders;
import io.github.huynhngochuyhoang.reliablemessage.core.serialization.MessageSerializer;
import io.github.huynhngochuyhoang.reliablemessage.mvc.IdempotencyStartResult;
import io.github.huynhngochuyhoang.reliablemessage.mvc.IdempotencyStore;
import io.github.huynhngochuyhoang.reliablemessage.observability.MessageMdc;
import io.github.huynhngochuyhoang.reliablemessage.observability.MessageObservability;
import io.github.huynhngochuyhoang.reliablemessage.observability.MessageTags;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.observation.ObservationRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;

import java.time.Duration;
import java.time.Instant;

public class KafkaReliableMessageHandler implements AcknowledgingMessageListener<String, byte[]> {

    private final KafkaReliableListenerEndpoint endpoint;
    private final MessageSerializer serializer;
    private final KafkaReliableListenerMethodInvoker invoker;
    private final MessageObservability observability;
    private final IdempotencyStore idempotencyStore;
    private final Duration idempotencyTtl;
    private final KafkaRetryStrategy retryStrategy;

    public KafkaReliableMessageHandler(
            KafkaReliableListenerEndpoint endpoint,
            MessageSerializer serializer,
            MeterRegistry meterRegistry
    ) {
        this(endpoint, serializer, new MessageObservability(meterRegistry, ObservationRegistry.NOOP), null, Duration.ofHours(24), null);
    }

    public KafkaReliableMessageHandler(
            KafkaReliableListenerEndpoint endpoint,
            MessageSerializer serializer,
            MeterRegistry meterRegistry,
            IdempotencyStore idempotencyStore,
            Duration idempotencyTtl
    ) {
        this(endpoint, serializer, new MessageObservability(meterRegistry, ObservationRegistry.NOOP), idempotencyStore, idempotencyTtl, null);
    }

    public KafkaReliableMessageHandler(
            KafkaReliableListenerEndpoint endpoint,
            MessageSerializer serializer,
            MessageObservability observability,
            IdempotencyStore idempotencyStore,
            Duration idempotencyTtl,
            KafkaRetryStrategy retryStrategy
    ) {
        this.endpoint = endpoint;
        this.serializer = serializer;
        this.invoker = new KafkaReliableListenerMethodInvoker(endpoint.bean(), endpoint.method());
        this.observability = observability;
        this.idempotencyStore = idempotencyStore;
        this.idempotencyTtl = idempotencyTtl == null ? Duration.ofHours(24) : idempotencyTtl;
        this.retryStrategy = retryStrategy;
    }

    @Override
    public void onMessage(ConsumerRecord<String, byte[]> record, Acknowledgment acknowledgment) {
        boolean idempotencyStarted = false;
        ReliableMessage<?> reliableMessage = null;
        try {
            if (!isRetryDue(record, acknowledgment)) {
                return;
            }
            reliableMessage = serializer.deserialize(record.value(), endpoint.payloadType());
            try (MessageMdc.Scope ignored = MessageMdc.apply(reliableMessage.headers())) {
                if (isIdempotencyEnabled(reliableMessage)) {
                    String idempotencyKey = reliableMessage.idempotencyKey();
                    IdempotencyStartResult startResult = observability.observe(
                            "message.idempotency.check",
                            "message_idempotency_check_duration",
                            MessageTags.mvcKafka(endpoint.eventName(), endpoint.consumerGroup(), "check"),
                            () -> idempotencyStore.tryStart(idempotencyKey, idempotencyTtl)
                    );
                    if (!startResult.started()) {
                        observability.increment("message_duplicate_total", MessageTags.mvcKafka(endpoint.eventName(), endpoint.consumerGroup(), "duplicate"));
                        consumeCounter("duplicate");
                        acknowledgment.acknowledge();
                        return;
                    }
                    idempotencyStarted = true;
                }

                ReliableMessage<?> currentMessage = reliableMessage;
                observability.observe(
                        "message.consume",
                        "message_consume_duration",
                        MessageTags.mvcKafka(endpoint.eventName(), endpoint.consumerGroup(), "success"),
                        () -> invoker.invoke(currentMessage)
                );
                if (idempotencyStarted) {
                    idempotencyStore.markSuccess(reliableMessage.idempotencyKey());
                }
                consumeCounter("success");
                acknowledgment.acknowledge();
            }
        } catch (RuntimeException error) {
            if (idempotencyStarted) {
                markFailed(reliableMessage, error);
            }
            consumeCounter("failed");
            routeFailure(record, acknowledgment, error);
        }
    }

    private boolean isRetryDue(ConsumerRecord<String, byte[]> record, Acknowledgment acknowledgment) {
        String retryNotBefore = KafkaRecordHeaders.value(record.headers(), ReliableMessageHeaders.RETRY_NOT_BEFORE);
        if (retryNotBefore == null || retryNotBefore.isBlank()) {
            return true;
        }
        long delayMillis = Long.parseLong(retryNotBefore) - Instant.now().toEpochMilli();
        if (delayMillis <= 0) {
            return true;
        }
        acknowledgment.nack(Duration.ofMillis(delayMillis));
        return false;
    }

    private boolean isIdempotencyEnabled(ReliableMessage<?> reliableMessage) {
        return idempotencyStore != null
                && reliableMessage.idempotencyKey() != null
                && !reliableMessage.idempotencyKey().isBlank();
    }

    private void markFailed(ReliableMessage<?> reliableMessage, RuntimeException error) {
        try {
            idempotencyStore.markFailed(reliableMessage.idempotencyKey(), error);
        } catch (RuntimeException markFailedError) {
            error.addSuppressed(markFailedError);
        }
    }

    private void routeFailure(ConsumerRecord<String, byte[]> record, Acknowledgment acknowledgment, RuntimeException error) {
        if (retryStrategy == null) {
            throw error;
        }
        try {
            retryStrategy.routeFailure(record, endpoint, error);
            acknowledgment.acknowledge();
        } catch (RuntimeException routeError) {
            error.addSuppressed(routeError);
            throw error;
        }
    }

    private void consumeCounter(String status) {
        observability.increment("message_consume_total", MessageTags.mvcKafka(endpoint.eventName(), endpoint.consumerGroup(), status));
        if ("failed".equals(status)) {
            observability.increment("message_consume_failed_total", MessageTags.mvcKafka(endpoint.eventName(), endpoint.consumerGroup(), status));
        }
    }
}
