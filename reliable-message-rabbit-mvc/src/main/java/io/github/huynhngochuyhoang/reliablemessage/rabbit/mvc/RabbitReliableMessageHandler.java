package io.github.huynhngochuyhoang.reliablemessage.rabbit.mvc;

import com.rabbitmq.client.Channel;
import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessage;
import io.github.huynhngochuyhoang.reliablemessage.core.serialization.MessageSerializer;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;

import java.io.IOException;

public class RabbitReliableMessageHandler implements ChannelAwareMessageListener {

    private final RabbitReliableListenerEndpoint endpoint;
    private final MessageSerializer serializer;
    private final RabbitReliableListenerMethodInvoker invoker;
    private final MeterRegistry meterRegistry;

    public RabbitReliableMessageHandler(
            RabbitReliableListenerEndpoint endpoint,
            MessageSerializer serializer,
            MeterRegistry meterRegistry
    ) {
        this.endpoint = endpoint;
        this.serializer = serializer;
        this.invoker = new RabbitReliableListenerMethodInvoker(endpoint.bean(), endpoint.method());
        this.meterRegistry = meterRegistry;
    }

    @Override
    public void onMessage(Message message, Channel channel) throws IOException {
        try {
            ReliableMessage<?> reliableMessage = serializer.deserialize(message.getBody(), endpoint.payloadType());
            invoker.invoke(reliableMessage);
            consumeCounter("success").increment();
            channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
        } catch (RuntimeException error) {
            consumeCounter("failed").increment();
            channel.basicNack(message.getMessageProperties().getDeliveryTag(), false, true);
            throw error;
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
