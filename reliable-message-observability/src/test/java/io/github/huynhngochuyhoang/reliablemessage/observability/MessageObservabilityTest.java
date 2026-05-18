package io.github.huynhngochuyhoang.reliablemessage.observability;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.observation.ObservationRegistry;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
}
