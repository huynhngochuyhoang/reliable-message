package io.github.huynhngochuyhoang.reliablemessage.kafka.webflux;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import java.nio.charset.StandardCharsets;

final class ReactiveKafkaRecordHeaders {

    private ReactiveKafkaRecordHeaders() {
    }

    static void put(Headers headers, String name, String value) {
        if (value != null && !value.isBlank()) {
            headers.remove(name);
            headers.add(name, value.getBytes(StandardCharsets.UTF_8));
        }
    }

    static String value(Headers headers, String name) {
        Header header = headers.lastHeader(name);
        if (header == null || header.value() == null) {
            return null;
        }
        return new String(header.value(), StandardCharsets.UTF_8);
    }
}
