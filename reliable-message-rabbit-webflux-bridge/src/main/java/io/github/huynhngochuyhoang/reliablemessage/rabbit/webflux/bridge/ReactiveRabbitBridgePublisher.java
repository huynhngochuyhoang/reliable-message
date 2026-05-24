package io.github.huynhngochuyhoang.reliablemessage.rabbit.webflux.bridge;

import io.github.huynhngochuyhoang.reliablemessage.core.PublishOptions;
import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessage;
import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessageHeaders;
import io.github.huynhngochuyhoang.reliablemessage.core.serialization.MessageSerializer;
import io.github.huynhngochuyhoang.reliablemessage.webflux.ReactiveReliablePublisher;
import io.github.huynhngochuyhoang.reliablemessage.webflux.ReliableMessageReactorContext;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import reactor.core.publisher.Mono;

import java.time.Clock;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class ReactiveRabbitBridgePublisher implements ReactiveReliablePublisher {

    private final RabbitTemplate rabbitTemplate;
    private final MessageSerializer serializer;
    private final RabbitWebFluxBridgeProperties properties;
    private final RabbitBridgeExecutorProvider executorProvider;
    private final RabbitBridgeConcurrencyGuard concurrencyGuard;
    private final Clock clock;
    private final RabbitBridgeEventLoopDetector eventLoopDetector;
    private final RabbitBridgeSafetyReporter safetyReporter;
    private final RabbitBridgeMetrics metrics;

    public ReactiveRabbitBridgePublisher(
            RabbitTemplate rabbitTemplate,
            MessageSerializer serializer,
            RabbitWebFluxBridgeProperties properties,
            RabbitBridgeExecutorProvider executorProvider,
            RabbitBridgeConcurrencyGuard concurrencyGuard,
            Clock clock
    ) {
        this(
                rabbitTemplate,
                serializer,
                properties,
                executorProvider,
                concurrencyGuard,
                clock,
                new RabbitBridgeEventLoopDetector(),
                RabbitBridgeSafetyReporter.logging(),
                RabbitBridgeMetrics.noop()
        );
    }

    public ReactiveRabbitBridgePublisher(
            RabbitTemplate rabbitTemplate,
            MessageSerializer serializer,
            RabbitWebFluxBridgeProperties properties,
            RabbitBridgeExecutorProvider executorProvider,
            RabbitBridgeConcurrencyGuard concurrencyGuard,
            Clock clock,
            RabbitBridgeEventLoopDetector eventLoopDetector,
            RabbitBridgeSafetyReporter safetyReporter
    ) {
        this(
                rabbitTemplate,
                serializer,
                properties,
                executorProvider,
                concurrencyGuard,
                clock,
                eventLoopDetector,
                safetyReporter,
                RabbitBridgeMetrics.noop()
        );
    }

    public ReactiveRabbitBridgePublisher(
            RabbitTemplate rabbitTemplate,
            MessageSerializer serializer,
            RabbitWebFluxBridgeProperties properties,
            RabbitBridgeExecutorProvider executorProvider,
            RabbitBridgeConcurrencyGuard concurrencyGuard,
            Clock clock,
            RabbitBridgeEventLoopDetector eventLoopDetector,
            RabbitBridgeSafetyReporter safetyReporter,
            RabbitBridgeMetrics metrics
    ) {
        this.rabbitTemplate = Objects.requireNonNull(rabbitTemplate, "rabbitTemplate");
        this.serializer = Objects.requireNonNull(serializer, "serializer");
        this.properties = Objects.requireNonNull(properties, "properties");
        this.executorProvider = Objects.requireNonNull(executorProvider, "executorProvider");
        this.concurrencyGuard = Objects.requireNonNull(concurrencyGuard, "concurrencyGuard");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.eventLoopDetector = Objects.requireNonNull(eventLoopDetector, "eventLoopDetector");
        this.safetyReporter = Objects.requireNonNull(safetyReporter, "safetyReporter");
        this.metrics = metrics == null ? RabbitBridgeMetrics.noop() : metrics;
    }

    @Override
    public Mono<Void> publish(String eventName, Object payload, PublishOptions options) {
        return Mono.deferContextual(context -> {
            reportEventLoopCallerIfNeeded(Thread.currentThread());
            PublishOptions contextOptions = ReliableMessageReactorContext.applyTo(
                    options == null ? PublishOptions.empty() : options,
                    context
            );
            ReliableMessage<Object> reliableMessage;
            Message message;
            try {
                reliableMessage = toReliableMessage(eventName, payload, contextOptions);
                message = toMessage(reliableMessage);
            } catch (RuntimeException error) {
                metrics.publish(eventName, "failure");
                return Mono.error(error);
            }
            return submitPublish(eventName, message);
        });
    }

    private void reportEventLoopCallerIfNeeded(Thread callerThread) {
        if (!eventLoopDetector.isEventLoopThread(callerThread)) {
            return;
        }
        try {
            safetyReporter.eventLoopPublishDetected(callerThread.getName());
        } catch (RuntimeException ignored) {
            // Safety reporting must not change publish behavior.
        }
    }

    private Message toMessage(ReliableMessage<Object> reliableMessage) {
        byte[] body = serializer.serialize(reliableMessage);
        MessageProperties messageProperties = new MessageProperties();
        messageProperties.setContentType(MessageProperties.CONTENT_TYPE_JSON);
        messageProperties.setDeliveryMode(MessageDeliveryMode.PERSISTENT);
        reliableMessage.headers().forEach(messageProperties::setHeader);
        return new Message(body, messageProperties);
    }

    private Mono<Void> submitPublish(String eventName, Message message) {
        return Mono.<Void>defer(() -> {
            CompletableFuture<Void> future;
            try {
                future = concurrencyGuard.submitFuture(executorProvider.getExecutor(), () -> {
                    publishOnBridgeExecutor(eventName, message);
                    return null;
                });
            } catch (RabbitBridgeRejectedException error) {
                metrics.executorRejected(eventName);
                metrics.publish(eventName, "failure");
                return Mono.error(error);
            } catch (RuntimeException error) {
                metrics.publish(eventName, "failure");
                return Mono.error(error);
            }
            return Mono.fromFuture(future)
                    .then(Mono.<Void>fromRunnable(() -> metrics.publish(eventName, "success")))
                    .onErrorMap(error -> {
                        metrics.publish(eventName, "failure");
                        return error;
                    });
        });
    }

    private void publishOnBridgeExecutor(String eventName, Message message) {
        rabbitTemplate.convertAndSend(properties.getRabbit().getExchange(), eventName, message);
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
