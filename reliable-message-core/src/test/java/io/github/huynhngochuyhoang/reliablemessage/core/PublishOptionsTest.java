package io.github.huynhngochuyhoang.reliablemessage.core;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PublishOptionsTest {

    @Test
    void buildsOptionsWithHeaders() {
        PublishOptions options = PublishOptions.builder()
                .aggregateId("order-1")
                .idempotencyKey("event-1")
                .correlationId("correlation-1")
                .partitionKey("order-1")
                .header("source", "orders")
                .build();

        assertEquals("order-1", options.aggregateId());
        assertEquals("event-1", options.idempotencyKey());
        assertEquals("correlation-1", options.correlationId());
        assertEquals("order-1", options.partitionKey());
        assertEquals("orders", options.headers().get("source"));
    }

    @Test
    void copiesHeadersDefensively() {
        Map<String, String> headers = new HashMap<>();
        headers.put("source", "orders");

        PublishOptions options = PublishOptions.builder()
                .headers(headers)
                .build();

        headers.put("source", "changed");

        assertEquals("orders", options.headers().get("source"));
        assertThrows(UnsupportedOperationException.class, () -> options.headers().put("other", "value"));
    }
}
