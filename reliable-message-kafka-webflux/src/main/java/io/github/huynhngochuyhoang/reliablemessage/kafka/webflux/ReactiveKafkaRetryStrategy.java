package io.github.huynhngochuyhoang.reliablemessage.kafka.webflux;

import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessageHeaders;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;

import java.time.Clock;
import java.time.Duration;
import java.util.List;

public class ReactiveKafkaRetryStrategy {

    private final KafkaSender<String, byte[]> kafkaSender;
    private final ReactiveKafkaReliableMessageProperties properties;
    private final Clock clock;

    public ReactiveKafkaRetryStrategy(
            KafkaSender<String, byte[]> kafkaSender,
            ReactiveKafkaReliableMessageProperties properties,
            Clock clock
    ) {
        this.kafkaSender = kafkaSender;
        this.properties = properties;
        this.clock = clock;
    }

    public Mono<Void> routeFailure(
            ReactiveKafkaReceivedRecord failedRecord,
            ReactiveKafkaReliableListenerEndpoint endpoint,
            Throwable error
    ) {
        int retryCount = retryCount(failedRecord) + 1;
        String route = routeName(retryCount);
        String topic = topicName(endpoint, retryCount);
        ProducerRecord<String, byte[]> record = copyForRepublish(failedRecord, topic, retryCount, error);
        if ("retry".equals(route)) {
            ReactiveKafkaRecordHeaders.put(record.headers(), ReliableMessageHeaders.RETRY_NOT_BEFORE,
                    String.valueOf(clock.instant().plus(backoff(retryCount)).toEpochMilli()));
        } else {
            record.headers().remove(ReliableMessageHeaders.RETRY_NOT_BEFORE);
        }
        return kafkaSender.send(Mono.just(SenderRecord.create(record, route)))
                .next()
                .flatMap(this::toCompletion);
    }

    private Mono<Void> toCompletion(SenderResult<String> result) {
        Exception exception = result.exception();
        if (exception != null) {
            return Mono.error(exception);
        }
        return Mono.empty();
    }

    private String routeName(int retryCount) {
        if (retryCount >= properties.getRetry().getAttempts() || properties.getRetry().getBackoff().isEmpty()) {
            return "dlt";
        }
        return "retry";
    }

    private String topicName(ReactiveKafkaReliableListenerEndpoint endpoint, int retryCount) {
        if (retryCount >= properties.getRetry().getAttempts() || properties.getRetry().getBackoff().isEmpty()) {
            return ReactiveKafkaTopicNames.dltTopic(endpoint.topicName(), endpoint.consumerGroup());
        }
        return ReactiveKafkaTopicNames.retryTopic(endpoint.topicName(), endpoint.consumerGroup(), backoff(retryCount));
    }

    private Duration backoff(int retryCount) {
        List<Duration> backoff = properties.getRetry().getBackoff();
        return backoff.get(Math.min(retryCount - 1, backoff.size() - 1));
    }

    static int retryCount(ReactiveKafkaReceivedRecord record) {
        String value = ReactiveKafkaRecordHeaders.value(record.headers(), ReliableMessageHeaders.RETRY_COUNT);
        if (value == null || value.isBlank()) {
            return 0;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException ignored) {
            return 0;
        }
    }

    static ProducerRecord<String, byte[]> copyForRepublish(
            ReactiveKafkaReceivedRecord source,
            String topic,
            int retryCount,
            Throwable error
    ) {
        ProducerRecord<String, byte[]> target = new ProducerRecord<>(topic, source.key(), source.value());
        for (Header header : source.headers()) {
            target.headers().add(header);
        }
        ReactiveKafkaRecordHeaders.put(target.headers(), ReliableMessageHeaders.RETRY_COUNT, String.valueOf(retryCount));
        if (ReactiveKafkaRecordHeaders.value(target.headers(), ReliableMessageHeaders.ORIGINAL_MESSAGE_ID) == null) {
            String messageId = ReactiveKafkaRecordHeaders.value(source.headers(), ReliableMessageHeaders.MESSAGE_ID);
            ReactiveKafkaRecordHeaders.put(target.headers(), ReliableMessageHeaders.ORIGINAL_MESSAGE_ID, messageId);
        }
        if (error != null) {
            ReactiveKafkaRecordHeaders.put(target.headers(), "x-error-type", error.getClass().getName());
            ReactiveKafkaRecordHeaders.put(target.headers(), "x-error-message", error.getMessage());
        }
        return target;
    }
}
