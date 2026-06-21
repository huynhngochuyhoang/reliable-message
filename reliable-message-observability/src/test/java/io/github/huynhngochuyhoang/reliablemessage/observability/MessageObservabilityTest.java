package io.github.huynhngochuyhoang.reliablemessage.observability;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.observation.ObservationRegistry;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class MessageObservabilityTest {

    @Test
    void recordsTaggedCounterAndDuration() {
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        MessageObservability observability = new MessageObservability(meterRegistry, ObservationRegistry.NOOP);
        MessageTags tags = MessageTags.mvcRabbit("order.created", "order-service.order.created", "success");

        observability.increment("message_consume_total", tags);
        observability.observe("message.consume", "message_consume_duration", tags, () -> {
        });

        assertEquals(1.0, meterRegistry.counter(
                "message_consume_total",
                "runtime", "mvc",
                "transport", "rabbit",
                "event_name", "order.created",
                "consumer", "order-service.order.created",
                "status", "success"
        ).count());
        assertEquals(1, meterRegistry.timer(
                "message_consume_duration",
                "runtime", "mvc",
                "transport", "rabbit",
                "event_name", "order.created",
                "consumer", "order-service.order.created",
                "status", "success"
        ).count());
    }


    @Test
    void repeatedOperationsReuseStableMetersAndAvoidHighCardinalityTags() {
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        MessageObservability observability = new MessageObservability(meterRegistry, ObservationRegistry.NOOP);
        MessageTags tags = MessageTags.mvcRabbit("order.created", "order-service.order.created", "success");

        observability.increment("message_consume_total", tags);
        observability.increment("message_consume_total", tags);

        assertEquals(2.0, meterRegistry.counter(
                "message_consume_total",
                "runtime", "mvc",
                "transport", "rabbit",
                "event_name", "order.created",
                "consumer", "order-service.order.created",
                "status", "success"
        ).count());
        assertEquals(1, meterRegistry.find("message_consume_total").counters().size());
        Set<String> tagKeys = meterRegistry.find("message_consume_total").counter()
                .getId()
                .getTags()
                .stream()
                .map(tag -> tag.getKey())
                .collect(java.util.stream.Collectors.toSet());
        assertFalse(tagKeys.contains("message_id"));
        assertFalse(tagKeys.contains("correlation_id"));
        assertFalse(tagKeys.contains("aggregate_id"));
        assertFalse(tagKeys.contains("idempotency_key"));
    }
}
