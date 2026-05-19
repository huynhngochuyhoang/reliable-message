package io.github.huynhngochuyhoang.reliablemessage.kafka.webflux;

import org.apache.kafka.common.header.Headers;

public interface ReactiveKafkaReceivedRecord {

    String topic();

    String key();

    byte[] value();

    Headers headers();

    ReactiveKafkaReceiverOffset receiverOffset();
}
