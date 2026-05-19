package io.github.huynhngochuyhoang.reliablemessage.kafka.webflux;

import reactor.kafka.receiver.KafkaReceiver;

import java.util.List;

public interface ReactiveKafkaReceiverFactory {

    KafkaReceiver<String, byte[]> create(List<String> topics, String consumerGroup);
}
