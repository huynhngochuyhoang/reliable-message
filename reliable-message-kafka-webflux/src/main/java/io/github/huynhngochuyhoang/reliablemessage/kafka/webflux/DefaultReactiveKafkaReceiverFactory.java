package io.github.huynhngochuyhoang.reliablemessage.kafka.webflux;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.List;

public class DefaultReactiveKafkaReceiverFactory implements ReactiveKafkaReceiverFactory {

    private final ReceiverOptions<String, byte[]> receiverOptions;

    public DefaultReactiveKafkaReceiverFactory(ReceiverOptions<String, byte[]> receiverOptions) {
        this.receiverOptions = receiverOptions;
    }

    @Override
    public KafkaReceiver<String, byte[]> create(List<String> topics, String consumerGroup) {
        return KafkaReceiver.create(receiverOptions
                .consumerProperty(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup)
                .subscription(topics));
    }
}
