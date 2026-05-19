package io.github.huynhngochuyhoang.reliablemessage.kafka.mvc;

import io.github.huynhngochuyhoang.reliablemessage.core.PublishOptions;
import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessage;
import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessageHeaders;
import io.github.huynhngochuyhoang.reliablemessage.core.serialization.MessageSerializer;
import io.github.huynhngochuyhoang.reliablemessage.mvc.ReliablePublisher;
import io.github.huynhngochuyhoang.reliablemessage.observability.MessageMdc;
import io.github.huynhngochuyhoang.reliablemessage.observability.MessageObservability;
import io.github.huynhngochuyhoang.reliablemessage.observability.MessageTags;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.observation.ObservationRegistry;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Clock;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class KafkaReliablePublisher implements ReliablePublisher {

    private final KafkaTemplate<String, byte[]> kafkaTemplate;
    private final MessageSerializer serializer;
    private final KafkaReliableMessageProperties properties;
    private final Clock clock;
    private final MessageObservability observability;

    public KafkaReliablePublisher(
            KafkaTemplate<String, byte[]> kafkaTemplate,
            MessageSerializer serializer,
            KafkaReliableMessageProperties properties,
            Clock clock,
            MeterRegistry meterRegistry
    ) {
        this(kafkaTemplate, serializer, properties, clock, new MessageObservability(meterRegistry, ObservationRegistry.NOOP));
    }

    public KafkaReliablePublisher(
            KafkaTemplate<String, byte[]> kafkaTemplate,
            MessageSerializer serializer,
            KafkaReliableMessageProperties properties,
            Clock clock,
            MessageObservability observability
    ) {
        this.kafkaTemplate = kafkaTemplate;
        this.serializer = serializer;
        this.properties = properties;
        this.clock = clock;
        this.observability = observability;
    }

    @Override
    public void publish(String eventName, Object payload, PublishOptions options) {
        PublishOptions safeOptions = options == null ? PublishOptions.empty() : options;
        ReliableMessage<Object> reliableMessage = toReliableMessage(eventName, payload, safeOptions);
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(
                properties.topicName(eventName),
                safeOptions.partitionKey(),
                serializer.serialize(reliableMessage)
        );
        reliableMessage.headers().forEach((name, value) -> KafkaRecordHeaders.put(record.headers(), name, value));

        try {
            observability.observe(
                    "message.publish",
                    null,
                    MessageTags.mvcKafka(eventName, null, "success"),
                    () -> send(record)
            );
            observability.increment("message_publish_total", MessageTags.mvcKafka(eventName, null, "success"));
        } catch (RuntimeException error) {
            observability.increment("message_publish_total", MessageTags.mvcKafka(eventName, null, "failed"));
            observability.increment("message_publish_failed_total", MessageTags.mvcKafka(eventName, null, "failed"));
            throw error;
        }
    }

    private ReliableMessage<Object> toReliableMessage(String eventName, Object payload, PublishOptions options) {
        String messageId = UUID.randomUUID().toString();
        Map<String, String> headers = new LinkedHashMap<>(options.headers());
        putIfPresent(headers, ReliableMessageHeaders.MESSAGE_ID, messageId);
        putIfPresent(headers, ReliableMessageHeaders.EVENT_NAME, eventName);
        putIfPresent(headers, ReliableMessageHeaders.AGGREGATE_ID, options.aggregateId());
        putIfPresent(headers, ReliableMessageHeaders.IDEMPOTENCY_KEY, options.idempotencyKey());
        putIfPresent(headers, ReliableMessageHeaders.CORRELATION_ID, options.correlationId());
        putIfPresent(headers, ReliableMessageHeaders.TRACE_ID, traceId(headers));
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

    private void send(ProducerRecord<String, byte[]> record) {
        try {
            kafkaTemplate.send(record).get(
                    properties.getKafka().getPublishTimeout().toMillis(),
                    TimeUnit.MILLISECONDS
            );
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while publishing Kafka reliable message", error);
        } catch (ExecutionException | TimeoutException error) {
            throw new IllegalStateException("Failed to publish Kafka reliable message", error);
        }
    }

    private static void putIfPresent(Map<String, String> headers, String name, String value) {
        if (value != null && !value.isBlank()) {
            headers.put(name, value);
        }
    }

    private static String traceId(Map<String, String> headers) {
        String traceId = headers.get(ReliableMessageHeaders.TRACE_ID);
        return traceId == null || traceId.isBlank() ? MessageMdc.currentTraceId() : traceId;
    }
}
