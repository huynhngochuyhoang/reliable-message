package io.github.huynhngochuyhoang.reliablemessage.rabbit.mvc;

import com.rabbitmq.client.Channel;
import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessage;
import io.github.huynhngochuyhoang.reliablemessage.core.serialization.MessageSerializer;
import io.github.huynhngochuyhoang.reliablemessage.mvc.IdempotencyStartResult;
import io.github.huynhngochuyhoang.reliablemessage.mvc.IdempotencyStore;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;

import java.io.IOException;
import java.time.Duration;

public class RabbitReliableMessageHandler implements ChannelAwareMessageListener {

    private final RabbitReliableListenerEndpoint endpoint;
    private final MessageSerializer serializer;
    private final RabbitReliableListenerMethodInvoker invoker;
    private final MeterRegistry meterRegistry;
    private final IdempotencyStore idempotencyStore;
    private final Duration idempotencyTtl;

    public RabbitReliableMessageHandler(
            RabbitReliableListenerEndpoint endpoint,
            MessageSerializer serializer,
            MeterRegistry meterRegistry
    ) {
        this(endpoint, serializer, meterRegistry, null, Duration.ofHours(24));
    }

    public RabbitReliableMessageHandler(
            RabbitReliableListenerEndpoint endpoint,
            MessageSerializer serializer,
            MeterRegistry meterRegistry,
            IdempotencyStore idempotencyStore,
            Duration idempotencyTtl
    ) {
        this.endpoint = endpoint;
        this.serializer = serializer;
        this.invoker = new RabbitReliableListenerMethodInvoker(endpoint.bean(), endpoint.method());
        this.meterRegistry = meterRegistry;
        this.idempotencyStore = idempotencyStore;
        this.idempotencyTtl = idempotencyTtl == null ? Duration.ofHours(24) : idempotencyTtl;
    }

    @Override
    public void onMessage(Message message, Channel channel) throws IOException {
        boolean idempotencyStarted = false;
        ReliableMessage<?> reliableMessage = null;
        try {
            reliableMessage = serializer.deserialize(message.getBody(), endpoint.payloadType());
            if (isIdempotencyEnabled(reliableMessage)) {
                IdempotencyStartResult startResult = idempotencyStore.tryStart(
                        reliableMessage.idempotencyKey(),
                        idempotencyTtl
                );
                if (!startResult.started()) {
                    consumeCounter("duplicate").increment();
                    channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
                    return;
                }
                idempotencyStarted = true;
            }

            invoker.invoke(reliableMessage);
            if (idempotencyStarted) {
                idempotencyStore.markSuccess(reliableMessage.idempotencyKey());
            }
            consumeCounter("success").increment();
            channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
        } catch (RuntimeException error) {
            if (idempotencyStarted) {
                markFailed(reliableMessage, error);
            }
            consumeCounter("failed").increment();
            channel.basicNack(message.getMessageProperties().getDeliveryTag(), false, true);
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

    private Counter consumeCounter(String status) {
        return Counter.builder("message_consume_total")
                .tag("runtime", "mvc")
                .tag("transport", "rabbit")
                .tag("event_name", endpoint.eventName())
                .tag("consumer", endpoint.queueName())
                .tag("status", status)
                .register(meterRegistry);
    }
}
