package io.github.huynhngochuyhoang.reliablemessage.kafka.mvc;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;

class KafkaTopicNamesTest {

    @Test
    void formatsRetryAndDltTopicNames() {
        assertEquals(
                "app.order.created.order-service.retry.30s",
                KafkaTopicNames.retryTopic("app.order.created", "order-service", Duration.ofSeconds(30))
        );
        assertEquals(
                "app.order.created.order-service.retry.1m",
                KafkaTopicNames.retryTopic("app.order.created", "order-service", Duration.ofMinutes(1))
        );
        assertEquals(
                "app.order.created.order-service.dlt",
                KafkaTopicNames.dltTopic("app.order.created", "order-service")
        );
    }
}
