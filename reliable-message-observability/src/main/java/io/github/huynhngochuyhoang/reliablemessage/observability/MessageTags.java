package io.github.huynhngochuyhoang.reliablemessage.observability;

import io.micrometer.core.instrument.Tags;

public record MessageTags(
        String runtime,
        String transport,
        String eventName,
        String consumer,
        String status
) {

    public static MessageTags mvcRabbit(String eventName, String consumer, String status) {
        return new MessageTags("mvc", "rabbit", eventName, consumer, status);
    }

    public static MessageTags mvcKafka(String eventName, String consumer, String status) {
        return new MessageTags("mvc", "kafka", eventName, consumer, status);
    }

    public Tags toMicrometerTags() {
        Tags tags = Tags.of(
                "runtime", value(runtime),
                "transport", value(transport),
                "event_name", value(eventName)
        );
        if (consumer != null && !consumer.isBlank()) {
            tags = tags.and("consumer", consumer);
        }
        if (status != null && !status.isBlank()) {
            tags = tags.and("status", status);
        }
        return tags;
    }

    private static String value(String value) {
        return value == null || value.isBlank() ? "unknown" : value;
    }
}
