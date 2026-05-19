package io.github.huynhngochuyhoang.reliablemessage.rabbit.mvc;

import io.github.huynhngochuyhoang.reliablemessage.core.DeadLetterRecord;
import io.github.huynhngochuyhoang.reliablemessage.core.MessageError;
import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessageHeaders;
import io.github.huynhngochuyhoang.reliablemessage.core.RetryMetadata;
import io.github.huynhngochuyhoang.reliablemessage.observability.MessageObservability;
import io.github.huynhngochuyhoang.reliablemessage.observability.MessageTags;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.observation.ObservationRegistry;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import java.time.Clock;
import java.time.Instant;
import java.util.UUID;

public class RabbitDlqService {

    private final RabbitTemplate rabbitTemplate;
    private final RabbitReliableMessageProperties properties;
    private final Clock clock;
    private final MessageObservability observability;

    public RabbitDlqService(
            RabbitTemplate rabbitTemplate,
            RabbitReliableMessageProperties properties,
            Clock clock,
            MeterRegistry meterRegistry
    ) {
        this(rabbitTemplate, properties, clock, new MessageObservability(meterRegistry, ObservationRegistry.NOOP));
    }

    public RabbitDlqService(
            RabbitTemplate rabbitTemplate,
            RabbitReliableMessageProperties properties,
            Clock clock,
            MessageObservability observability
    ) {
        this.rabbitTemplate = rabbitTemplate;
        this.properties = properties;
        this.clock = clock;
        this.observability = observability;
    }

    public void retry(String eventName, Message dlqMessage) {
        Message message = RabbitRetryStrategy.copyForRepublish(dlqMessage, 0, null);
        observability.observe(
                "message.dlq",
                null,
                MessageTags.mvcRabbit(eventName, properties.queueName(eventName), "retry"),
                () -> rabbitTemplate.send(properties.getRabbit().getExchange(), eventName, message)
        );
        operationCounter("retry", eventName, properties.queueName(eventName));
    }

    public DeadLetterRecord discard(String eventName, String consumer, Message dlqMessage, String reason) {
        Instant now = clock.instant();
        DeadLetterRecord record = new DeadLetterRecord(
                UUID.randomUUID().toString(),
                messageId(dlqMessage),
                eventName,
                consumer,
                "rabbit",
                RabbitRetryStrategy.payload(dlqMessage),
                RabbitRetryStrategy.stringHeaders(dlqMessage),
                new RetryMetadata(
                        RabbitRetryStrategy.retryCount(dlqMessage),
                        properties.getRetry().getAttempts(),
                        null
                ),
                new MessageError("discarded", reason, now),
                now
        );
        operationCounter("discard", eventName, consumer);
        return record;
    }

    private String messageId(Message message) {
        Object header = message.getMessageProperties().getHeaders().get(ReliableMessageHeaders.MESSAGE_ID);
        if (header != null && !String.valueOf(header).isBlank()) {
            return String.valueOf(header);
        }
        String messageId = message.getMessageProperties().getMessageId();
        return messageId == null || messageId.isBlank() ? UUID.randomUUID().toString() : messageId;
    }

    private void operationCounter(String operation, String eventName, String consumer) {
        observability.increment("message_dlq_operations_total", MessageTags.mvcRabbit(eventName, consumer, operation));
        observability.increment("message_dlq_total", MessageTags.mvcRabbit(eventName, consumer, operation));
    }
}
