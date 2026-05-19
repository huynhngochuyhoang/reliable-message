package io.github.huynhngochuyhoang.reliablemessage.kafka.mvc;

import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessageHeaders;
import io.github.huynhngochuyhoang.reliablemessage.observability.MessageObservability;
import io.github.huynhngochuyhoang.reliablemessage.observability.MessageTags;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.observation.ObservationRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class KafkaRetryStrategy {

    private final KafkaTemplate<String, byte[]> kafkaTemplate;
    private final KafkaReliableMessageProperties properties;
    private final MessageObservability observability;
    private final Clock clock;

    public KafkaRetryStrategy(
            KafkaTemplate<String, byte[]> kafkaTemplate,
            KafkaReliableMessageProperties properties,
            MeterRegistry meterRegistry
    ) {
        this(kafkaTemplate, properties, new MessageObservability(meterRegistry, ObservationRegistry.NOOP));
    }

    public KafkaRetryStrategy(
            KafkaTemplate<String, byte[]> kafkaTemplate,
            KafkaReliableMessageProperties properties,
            MessageObservability observability
    ) {
        this(kafkaTemplate, properties, observability, Clock.systemUTC());
    }

    KafkaRetryStrategy(
            KafkaTemplate<String, byte[]> kafkaTemplate,
            KafkaReliableMessageProperties properties,
            MessageObservability observability,
            Clock clock
    ) {
        this.kafkaTemplate = kafkaTemplate;
        this.properties = properties;
        this.observability = observability;
        this.clock = clock;
    }

    public void routeFailure(ConsumerRecord<String, byte[]> failedRecord, KafkaReliableListenerEndpoint endpoint, Throwable error) {
        int retryCount = retryCount(failedRecord) + 1;
        String route = routeName(retryCount);
        String topic = topicName(endpoint, retryCount);
        ProducerRecord<String, byte[]> record = copyForRepublish(failedRecord, topic, retryCount, error);
        if ("retry".equals(route)) {
            KafkaRecordHeaders.put(record.headers(), ReliableMessageHeaders.RETRY_NOT_BEFORE,
                    String.valueOf(clock.instant().plus(backoff(retryCount)).toEpochMilli()));
        } else {
            record.headers().remove(ReliableMessageHeaders.RETRY_NOT_BEFORE);
        }
        observability.observe(
                "retry".equals(route) ? "message.retry" : "message.dlq",
                null,
                MessageTags.mvcKafka(endpoint.eventName(), endpoint.consumerGroup(), route),
                () -> send(record)
        );
        observability.increment("message_consume_failure_routed_total", MessageTags.mvcKafka(endpoint.eventName(), endpoint.consumerGroup(), route));
        observability.increment("retry".equals(route) ? "message_retry_total" : "message_dlq_total",
                MessageTags.mvcKafka(endpoint.eventName(), endpoint.consumerGroup(), route));
    }

    private String routeName(int retryCount) {
        if (retryCount >= properties.getRetry().getAttempts() || properties.getRetry().getBackoff().isEmpty()) {
            return "dlt";
        }
        return "retry";
    }

    private String topicName(KafkaReliableListenerEndpoint endpoint, int retryCount) {
        if (retryCount >= properties.getRetry().getAttempts() || properties.getRetry().getBackoff().isEmpty()) {
            return KafkaTopicNames.dltTopic(endpoint.topicName(), endpoint.consumerGroup());
        }
        return KafkaTopicNames.retryTopic(endpoint.topicName(), endpoint.consumerGroup(), backoff(retryCount));
    }

    private Duration backoff(int retryCount) {
        List<Duration> backoff = properties.getRetry().getBackoff();
        return backoff.get(Math.min(retryCount - 1, backoff.size() - 1));
    }

    private void send(ProducerRecord<String, byte[]> record) {
        try {
            kafkaTemplate.send(record).get(
                    properties.getKafka().getPublishTimeout().toMillis(),
                    TimeUnit.MILLISECONDS
            );
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while routing Kafka reliable message failure", error);
        } catch (ExecutionException | TimeoutException error) {
            throw new IllegalStateException("Failed to route Kafka reliable message failure", error);
        }
    }

    static int retryCount(ConsumerRecord<String, byte[]> record) {
        String value = KafkaRecordHeaders.value(record.headers(), ReliableMessageHeaders.RETRY_COUNT);
        return value == null || value.isBlank() ? 0 : Integer.parseInt(value);
    }

    static ProducerRecord<String, byte[]> copyForRepublish(
            ConsumerRecord<String, byte[]> source,
            String topic,
            int retryCount,
            Throwable error
    ) {
        ProducerRecord<String, byte[]> target = new ProducerRecord<>(topic, source.key(), source.value());
        for (Header header : source.headers()) {
            target.headers().add(header);
        }
        KafkaRecordHeaders.put(target.headers(), ReliableMessageHeaders.RETRY_COUNT, String.valueOf(retryCount));
        if (KafkaRecordHeaders.value(target.headers(), ReliableMessageHeaders.ORIGINAL_MESSAGE_ID) == null) {
            String messageId = KafkaRecordHeaders.value(source.headers(), ReliableMessageHeaders.MESSAGE_ID);
            KafkaRecordHeaders.put(target.headers(), ReliableMessageHeaders.ORIGINAL_MESSAGE_ID, messageId);
        }
        if (error != null) {
            KafkaRecordHeaders.put(target.headers(), "x-error-type", error.getClass().getName());
            KafkaRecordHeaders.put(target.headers(), "x-error-message", error.getMessage());
        }
        return target;
    }
}
