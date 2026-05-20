package io.github.huynhngochuyhoang.reliablemessage.rabbit.mvc;

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
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
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
    private final MessageObservability observability;

    public RabbitReliablePublisher(
            RabbitTemplate rabbitTemplate,
            MessageSerializer serializer,
            RabbitReliableMessageProperties properties,
            Clock clock,
            MeterRegistry meterRegistry
    ) {
        this(rabbitTemplate, serializer, properties, clock, new MessageObservability(meterRegistry, ObservationRegistry.NOOP));
    }

    public RabbitReliablePublisher(
            RabbitTemplate rabbitTemplate,
            MessageSerializer serializer,
            RabbitReliableMessageProperties properties,
            Clock clock,
            MessageObservability observability
    ) {
        this.rabbitTemplate = rabbitTemplate;
        this.serializer = serializer;
        this.properties = properties;
        this.clock = clock;
        this.observability = observability;
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
            observability.observe(
                    "message.publish",
                    null,
                    MessageTags.mvcRabbit(eventName, null, "success"),
                    () -> send(eventName, new Message(body, messageProperties))
            );
            observability.increment("message_publish_total", MessageTags.mvcRabbit(eventName, null, "success"));
        } catch (RuntimeException error) {
            observability.increment("message_publish_total", MessageTags.mvcRabbit(eventName, null, "failed"));
            observability.increment("message_publish_failed_total", MessageTags.mvcRabbit(eventName, null, "failed"));
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

    private void send(String eventName, Message message) {
        String exchange = properties.getRabbit().getExchange();
        if (!shouldUsePublisherConfirm()) {
            rabbitTemplate.send(exchange, eventName, message);
            return;
        }

        long timeoutMillis = properties.getRabbit().getPublisherConfirmTimeout().toMillis();
        rabbitTemplate.invoke(operations -> {
            operations.send(exchange, eventName, message);
            operations.waitForConfirmsOrDie(timeoutMillis);
            return null;
        });
    }

    private boolean shouldUsePublisherConfirm() {
        if (!properties.getRabbit().isPublisherConfirm()) {
            return false;
        }
        if (!(rabbitTemplate.getConnectionFactory() instanceof CachingConnectionFactory cachingConnectionFactory)) {
            return false;
        }
        return cachingConnectionFactory.isPublisherConfirms() || cachingConnectionFactory.isSimplePublisherConfirms();
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
