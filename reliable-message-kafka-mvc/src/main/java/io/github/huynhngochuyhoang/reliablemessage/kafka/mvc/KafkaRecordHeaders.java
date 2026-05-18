package io.github.huynhngochuyhoang.reliablemessage.kafka.mvc;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import java.nio.charset.StandardCharsets;

final class KafkaRecordHeaders {

    private KafkaRecordHeaders() {
    }

    static void put(Headers headers, String name, String value) {
        if (value != null && !value.isBlank()) {
            headers.remove(name);
            headers.add(name, value.getBytes(StandardCharsets.UTF_8));
        }
    }

    static String value(Headers headers, String name) {
        Header header = headers.lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }
}
