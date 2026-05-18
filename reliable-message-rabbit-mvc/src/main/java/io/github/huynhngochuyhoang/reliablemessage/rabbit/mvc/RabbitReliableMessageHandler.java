package io.github.huynhngochuyhoang.reliablemessage.rabbit.mvc;

import com.rabbitmq.client.Channel;
import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessage;
import io.github.huynhngochuyhoang.reliablemessage.core.serialization.MessageSerializer;
import io.github.huynhngochuyhoang.reliablemessage.mvc.IdempotencyStartResult;
import io.github.huynhngochuyhoang.reliablemessage.mvc.IdempotencyStore;
import io.github.huynhngochuyhoang.reliablemessage.observability.MessageMdc;
import io.github.huynhngochuyhoang.reliablemessage.observability.MessageObservability;
import io.github.huynhngochuyhoang.reliablemessage.observability.MessageTags;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.observation.ObservationRegistry;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;

import java.io.IOException;
import java.time.Duration;

public class RabbitReliableMessageHandler implements ChannelAwareMessageListener {

    private final RabbitReliableListenerEndpoint endpoint;
    private final MessageSerializer serializer;
    private final RabbitReliableListenerMethodInvoker invoker;
    private final MessageObservability observability;
    private final IdempotencyStore idempotencyStore;
    private final Duration idempotencyTtl;
    private final RabbitRetryStrategy retryStrategy;

    public RabbitReliableMessageHandler(
            RabbitReliableListenerEndpoint endpoint,
            MessageSerializer serializer,
            MeterRegistry meterRegistry
    ) {
        this(endpoint, serializer, new MessageObservability(meterRegistry, ObservationRegistry.NOOP), null, Duration.ofHours(24), null);
    }

    public RabbitReliableMessageHandler(
            RabbitReliableListenerEndpoint endpoint,
            MessageSerializer serializer,
            MeterRegistry meterRegistry,
            IdempotencyStore idempotencyStore,
            Duration idempotencyTtl
    ) {
        this(endpoint, serializer, new MessageObservability(meterRegistry, ObservationRegistry.NOOP), idempotencyStore, idempotencyTtl, null);
    }

    public RabbitReliableMessageHandler(
            RabbitReliableListenerEndpoint endpoint,
            MessageSerializer serializer,
            MessageObservability observability,
            IdempotencyStore idempotencyStore,
            Duration idempotencyTtl,
            RabbitRetryStrategy retryStrategy
    ) {
        this.endpoint = endpoint;
        this.serializer = serializer;
        this.invoker = new RabbitReliableListenerMethodInvoker(endpoint.bean(), endpoint.method());
        this.observability = observability;
        this.idempotencyStore = idempotencyStore;
        this.idempotencyTtl = idempotencyTtl == null ? Duration.ofHours(24) : idempotencyTtl;
        this.retryStrategy = retryStrategy;
    }

    @Override
    public void onMessage(Message message, Channel channel) throws IOException {
        boolean idempotencyStarted = false;
        ReliableMessage<?> reliableMessage = null;
        try {
            reliableMessage = serializer.deserialize(message.getBody(), endpoint.payloadType());
            try (MessageMdc.Scope ignored = MessageMdc.apply(reliableMessage.headers())) {
                if (isIdempotencyEnabled(reliableMessage)) {
                    String idempotencyKey = reliableMessage.idempotencyKey();
                    IdempotencyStartResult startResult = observability.observe(
                            "message.idempotency.check",
                            "message_idempotency_check_duration",
                            MessageTags.mvcRabbit(endpoint.eventName(), endpoint.queueName(), "check"),
                            () -> idempotencyStore.tryStart(idempotencyKey, idempotencyTtl)
                    );
                    if (!startResult.started()) {
                        observability.increment("message_duplicate_total", MessageTags.mvcRabbit(endpoint.eventName(), endpoint.queueName(), "duplicate"));
                        consumeCounter("duplicate");
                        channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
                        return;
                    }
                    idempotencyStarted = true;
                }

                ReliableMessage<?> currentMessage = reliableMessage;
                observability.observe(
                        "message.consume",
                        "message_consume_duration",
                        MessageTags.mvcRabbit(endpoint.eventName(), endpoint.queueName(), "success"),
                        () -> invoker.invoke(currentMessage)
                );
                if (idempotencyStarted) {
                    idempotencyStore.markSuccess(reliableMessage.idempotencyKey());
                }
                consumeCounter("success");
                channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
            }
        } catch (RuntimeException error) {
            if (idempotencyStarted) {
                markFailed(reliableMessage, error);
            }
            consumeCounter("failed");
            routeFailure(message, channel, error);
            throw error;
        }
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

    private void routeFailure(Message message, Channel channel, RuntimeException error) throws IOException {
        if (retryStrategy == null) {
            channel.basicNack(message.getMessageProperties().getDeliveryTag(), false, true);
            return;
        }
        try {
            retryStrategy.routeFailure(message, endpoint, error);
            channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
        } catch (RuntimeException routeError) {
            error.addSuppressed(routeError);
            channel.basicNack(message.getMessageProperties().getDeliveryTag(), false, true);
        }
    }

    private void consumeCounter(String status) {
        observability.increment("message_consume_total", MessageTags.mvcRabbit(endpoint.eventName(), endpoint.queueName(), status));
        if ("failed".equals(status)) {
            observability.increment("message_consume_failed_total", MessageTags.mvcRabbit(endpoint.eventName(), endpoint.queueName(), status));
        }
    }
}
