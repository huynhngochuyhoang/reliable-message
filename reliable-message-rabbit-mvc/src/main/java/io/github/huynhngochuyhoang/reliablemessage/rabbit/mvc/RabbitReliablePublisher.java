package io.github.huynhngochuyhoang.reliablemessage.rabbit.mvc;

import io.github.huynhngochuyhoang.reliablemessage.core.PublishOptions;
import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessage;
import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessageHeaders;
import io.github.huynhngochuyhoang.reliablemessage.core.serialization.MessageSerializer;
import io.github.huynhngochuyhoang.reliablemessage.mvc.ReliablePublisher;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import java.time.Clock;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

public class RabbitReliablePublisher implements ReliablePublisher {

    private final RabbitTemplate rabbitTemplate;
    private final MessageSerializer serializer;
    private final RabbitReliableMessageProperties properties;
    private final Clock clock;
    private final MeterRegistry meterRegistry;

    public RabbitReliablePublisher(
            RabbitTemplate rabbitTemplate,
            MessageSerializer serializer,
            RabbitReliableMessageProperties properties,
            Clock clock,
            MeterRegistry meterRegistry
    ) {
        this.rabbitTemplate = rabbitTemplate;
        this.serializer = serializer;
        this.properties = properties;
        this.clock = clock;
        this.meterRegistry = meterRegistry;
    }

    @Override
    public void publish(String eventName, Object payload, PublishOptions options) {
        PublishOptions safeOptions = options == null ? PublishOptions.empty() : options;
        ReliableMessage<Object> reliableMessage = toReliableMessage(eventName, payload, safeOptions);
        byte[] body = serializer.serialize(reliableMessage);

        MessageProperties messageProperties = new MessageProperties();
        messageProperties.setContentType(MessageProperties.CONTENT_TYPE_JSON);
        messageProperties.setDeliveryMode(MessageDeliveryMode.PERSISTENT);
        reliableMessage.headers().forEach(messageProperties::setHeader);

        try {
            rabbitTemplate.send(properties.getRabbit().getExchange(), eventName, new Message(body, messageProperties));
            publishCounter(eventName, "success").increment();
        } catch (RuntimeException error) {
            publishCounter(eventName, "failed").increment();
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
        putIfPresent(headers, ReliableMessageHeaders.PARTITION_KEY, options.partitionKey());

        return new ReliableMessage<>(
                messageId,
                eventName,
                options.aggregateId(),
                options.idempotencyKey(),
                options.correlationId(),
                null,
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

    private Counter publishCounter(String eventName, String status) {
        return Counter.builder("message_publish_total")
                .tag("runtime", "mvc")
                .tag("transport", "rabbit")
                .tag("event_name", eventName)
                .tag("status", status)
                .register(meterRegistry);
    }
}
