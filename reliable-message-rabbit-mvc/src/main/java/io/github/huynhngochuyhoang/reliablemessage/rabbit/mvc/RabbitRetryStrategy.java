package io.github.huynhngochuyhoang.reliablemessage.rabbit.mvc;

import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessageHeaders;
import io.github.huynhngochuyhoang.reliablemessage.observability.MessageObservability;
import io.github.huynhngochuyhoang.reliablemessage.observability.MessageTags;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.observation.ObservationRegistry;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;

public class RabbitRetryStrategy {

    private final RabbitTemplate rabbitTemplate;
    private final RabbitReliableMessageProperties properties;
    private final MessageObservability observability;

    public RabbitRetryStrategy(
            RabbitTemplate rabbitTemplate,
            RabbitReliableMessageProperties properties,
            MeterRegistry meterRegistry
    ) {
        this(rabbitTemplate, properties, new MessageObservability(meterRegistry, ObservationRegistry.NOOP));
    }

    public RabbitRetryStrategy(
            RabbitTemplate rabbitTemplate,
            RabbitReliableMessageProperties properties,
            MessageObservability observability
    ) {
        this.rabbitTemplate = rabbitTemplate;
        this.properties = properties;
        this.observability = observability;
    }

    public void routeFailure(Message failedMessage, RabbitReliableListenerEndpoint endpoint, Throwable error) {
        int retryCount = retryCount(failedMessage) + 1;
        Message message = copyForRepublish(failedMessage, retryCount, error);
        String routingKey = routingKey(endpoint, retryCount);
        String route = routeName(endpoint, retryCount);
        observability.observe(
                "retry".equals(route) ? "message.retry" : "message.dlq",
                null,
                MessageTags.mvcRabbit(endpoint.eventName(), endpoint.queueName(), route),
                () -> rabbitTemplate.send(properties.getRabbit().getExchange(), routingKey, message)
        );
        observability.increment("message_consume_failure_routed_total", MessageTags.mvcRabbit(endpoint.eventName(), endpoint.queueName(), route));
        observability.increment("retry".equals(route) ? "message_retry_total" : "message_dlq_total",
                MessageTags.mvcRabbit(endpoint.eventName(), endpoint.queueName(), route));
    }

    private String routingKey(RabbitReliableListenerEndpoint endpoint, int retryCount) {
        if (retryCount >= properties.getRetry().getAttempts() || properties.getRetry().getBackoff().isEmpty()) {
            return RabbitTopologyNames.dlqRoutingKey(endpoint.queueName());
        }
        Duration delay = backoff(retryCount);
        return RabbitTopologyNames.retryRoutingKey(endpoint.queueName(), delay);
    }

    private String routeName(RabbitReliableListenerEndpoint endpoint, int retryCount) {
        if (retryCount >= properties.getRetry().getAttempts() || properties.getRetry().getBackoff().isEmpty()) {
            return "dlq";
        }
        return "retry";
    }

    private Duration backoff(int retryCount) {
        List<Duration> backoff = properties.getRetry().getBackoff();
        return backoff.get(Math.min(retryCount - 1, backoff.size() - 1));
    }

    static int retryCount(Message message) {
        Object value = message.getMessageProperties().getHeaders().get(ReliableMessageHeaders.RETRY_COUNT);
        if (value instanceof Number number) {
            return number.intValue();
        }
        if (value instanceof String text && !text.isBlank()) {
            return Integer.parseInt(text);
        }
        return 0;
    }

    static Message copyForRepublish(Message source, int retryCount, Throwable error) {
        MessageProperties sourceProperties = source.getMessageProperties();
        MessageProperties targetProperties = new MessageProperties();
        targetProperties.getHeaders().putAll(sourceProperties.getHeaders());
        targetProperties.setHeader(ReliableMessageHeaders.RETRY_COUNT, retryCount);
        if (!targetProperties.getHeaders().containsKey(ReliableMessageHeaders.ORIGINAL_MESSAGE_ID)) {
            Object messageId = sourceProperties.getHeaders().get(ReliableMessageHeaders.MESSAGE_ID);
            if (messageId == null) {
                messageId = sourceProperties.getMessageId();
            }
            if (messageId != null) {
                targetProperties.setHeader(ReliableMessageHeaders.ORIGINAL_MESSAGE_ID, messageId);
            }
        }
        if (error != null) {
            targetProperties.setHeader("x-error-type", error.getClass().getName());
            targetProperties.setHeader("x-error-message", error.getMessage());
        }
        targetProperties.setContentType(sourceProperties.getContentType());
        targetProperties.setContentEncoding(sourceProperties.getContentEncoding());
        targetProperties.setMessageId(sourceProperties.getMessageId());
        targetProperties.setCorrelationId(sourceProperties.getCorrelationId());
        MessageDeliveryMode deliveryMode = sourceProperties.getDeliveryMode();
        if (deliveryMode != null) {
            targetProperties.setDeliveryMode(deliveryMode);
        }
        return new Message(source.getBody(), targetProperties);
    }

    static Map<String, String> stringHeaders(Message message) {
        return message.getMessageProperties().getHeaders().entrySet().stream()
                .collect(java.util.stream.Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> String.valueOf(entry.getValue())
                ));
    }

    static String payload(Message message) {
        return new String(message.getBody(), StandardCharsets.UTF_8);
    }

}
