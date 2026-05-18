package io.github.huynhngochuyhoang.reliablemessage.kafka.mvc;

import java.time.Duration;

public final class KafkaTopicNames {

    private KafkaTopicNames() {
    }

    public static String retryTopic(String topicName, String consumerGroup, Duration delay) {
        return topicName + "." + consumerGroup + ".retry." + format(delay);
    }

    public static String dltTopic(String topicName, String consumerGroup) {
        return topicName + "." + consumerGroup + ".dlt";
    }

    static String format(Duration duration) {
        if (duration.toMillis() % 3_600_000 == 0) {
            return duration.toHours() + "h";
        }
        if (duration.toMillis() % 60_000 == 0) {
            return duration.toMinutes() + "m";
        }
        if (duration.toMillis() % 1_000 == 0) {
            return duration.toSeconds() + "s";
        }
        return duration.toMillis() + "ms";
    }
}
