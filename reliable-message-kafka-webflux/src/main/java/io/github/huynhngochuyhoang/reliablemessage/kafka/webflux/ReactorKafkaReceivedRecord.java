package io.github.huynhngochuyhoang.reliablemessage.kafka.webflux;

import org.apache.kafka.common.header.Headers;
import reactor.kafka.receiver.ReceiverRecord;

final class ReactorKafkaReceivedRecord implements ReactiveKafkaReceivedRecord {

    private final ReceiverRecord<String, byte[]> record;

    ReactorKafkaReceivedRecord(ReceiverRecord<String, byte[]> record) {
        this.record = record;
    }

    @Override
    public String topic() {
        return record.topic();
    }

    @Override
    public String key() {
        return record.key();
    }

    @Override
    public byte[] value() {
        return record.value();
    }

    @Override
    public Headers headers() {
        return record.headers();
    }

    @Override
    public ReactiveKafkaReceiverOffset receiverOffset() {
        return () -> record.receiverOffset().commit();
    }
}
