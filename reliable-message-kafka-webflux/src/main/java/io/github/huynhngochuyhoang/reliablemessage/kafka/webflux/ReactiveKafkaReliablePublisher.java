package io.github.huynhngochuyhoang.reliablemessage.kafka.webflux;

import io.github.huynhngochuyhoang.reliablemessage.core.PublishOptions;
import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessage;
import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessageHeaders;
import io.github.huynhngochuyhoang.reliablemessage.core.serialization.MessageSerializer;
import io.github.huynhngochuyhoang.reliablemessage.observability.MessageObservability;
import io.github.huynhngochuyhoang.reliablemessage.observability.MessageTags;
import io.github.huynhngochuyhoang.reliablemessage.webflux.ReactiveReliablePublisher;
import io.github.huynhngochuyhoang.reliablemessage.webflux.ReliableMessageReactorContext;
import org.apache.kafka.clients.producer.ProducerRecord;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;

import java.time.Clock;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

public class ReactiveKafkaReliablePublisher implements ReactiveReliablePublisher {

    private final KafkaSender<String, byte[]> kafkaSender;
    private final MessageSerializer serializer;
    private final ReactiveKafkaReliableMessageProperties properties;
    private final Clock clock;
    private final MessageObservability observability;

    public ReactiveKafkaReliablePublisher(
            KafkaSender<String, byte[]> kafkaSender,
            MessageSerializer serializer,
            ReactiveKafkaReliableMessageProperties properties,
            Clock clock
    ) {
        this(kafkaSender, serializer, properties, clock, null);
    }

    public ReactiveKafkaReliablePublisher(
            KafkaSender<String, byte[]> kafkaSender,
            MessageSerializer serializer,
            ReactiveKafkaReliableMessageProperties properties,
            Clock clock,
            MessageObservability observability
    ) {
        this.kafkaSender = kafkaSender;
        this.serializer = serializer;
        this.properties = properties;
        this.clock = clock;
        this.observability = observability;
    }

    @Override
    public Mono<Void> publish(String eventName, Object payload, PublishOptions options) {
        return Mono.deferContextual(context -> {
            PublishOptions contextOptions = ReliableMessageReactorContext.applyTo(
                    options == null ? PublishOptions.empty() : options,
                    context
            );
            ReliableMessage<Object> reliableMessage = toReliableMessage(eventName, payload, contextOptions);
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(
                    properties.topicName(eventName),
                    contextOptions.partitionKey(),
                    serializer.serialize(reliableMessage)
            );
            reliableMessage.headers().forEach((name, value) -> ReactiveKafkaRecordHeaders.put(record.headers(), name, value));
            return kafkaSender.send(Mono.just(SenderRecord.create(record, reliableMessage.messageId())))
                    .next()
                    .flatMap(this::toCompletion);
        })
                .doOnSuccess(ignored -> publishMetric(eventName, "success"))
                .doOnError(error -> {
                    publishMetric(eventName, "failed");
                    increment("message_publish_failed_total", eventName, "failed");
                });
    }

    private void publishMetric(String eventName, String status) {
        increment("message_publish_total", eventName, status);
    }

    private void increment(String metricName, String eventName, String status) {
        if (observability == null) {
            return;
        }
        try {
            observability.increment(metricName, MessageTags.webfluxKafka(eventName, null, status));
        } catch (RuntimeException ignored) {
            // Metrics must not change Kafka event flow.
        }
    }

    private Mono<Void> toCompletion(SenderResult<String> result) {
        Exception exception = result.exception();
        if (exception != null) {
            return Mono.error(exception);
        }
        return Mono.empty();
    }

    private ReliableMessage<Object> toReliableMessage(String eventName, Object payload, PublishOptions options) {
        String messageId = UUID.randomUUID().toString();
        Map<String, String> headers = new LinkedHashMap<>(options.headers());
        putIfPresent(headers, ReliableMessageHeaders.MESSAGE_ID, messageId);
        putIfPresent(headers, ReliableMessageHeaders.EVENT_NAME, eventName);
        putIfPresent(headers, ReliableMessageHeaders.AGGREGATE_ID, options.aggregateId());
        putIfPresent(headers, ReliableMessageHeaders.IDEMPOTENCY_KEY, options.idempotencyKey());
        putIfPresent(headers, ReliableMessageHeaders.CORRELATION_ID, options.correlationId());
        putIfPresent(headers, ReliableMessageHeaders.TRACE_ID, headers.get(ReliableMessageHeaders.TRACE_ID));
        putIfPresent(headers, ReliableMessageHeaders.PARTITION_KEY, options.partitionKey());

        return new ReliableMessage<>(
                messageId,
                eventName,
                options.aggregateId(),
                options.idempotencyKey(),
                options.correlationId(),
                headers.get(ReliableMessageHeaders.TRACE_ID),
                clock.instant(),
                headers,
                payload
        );
    }

    private static void putIfPresent(Map<String, String> headers, String name, String value) {
        if (value != null && !value.isBlank()) {
            headers.put(name, value);
        }
    }
}
