package io.github.huynhngochuyhoang.reliablemessage.rabbit.mvc;

import java.time.Duration;

final class RabbitTopologyNames {

    private RabbitTopologyNames() {
    }

    static String retryQueueName(String queueName, Duration delay) {
        return queueName + ".retry." + delaySuffix(delay);
    }

    static String retryRoutingKey(String queueName, Duration delay) {
        return retryQueueName(queueName, delay);
    }

    static String dlqQueueName(String queueName) {
        return queueName + ".dlq";
    }

    static String dlqRoutingKey(String queueName) {
        return dlqQueueName(queueName);
    }

    private static String delaySuffix(Duration delay) {
        if (delay.toMinutesPart() == 0 && delay.toHoursPart() == 0 && delay.toDaysPart() == 0) {
            return delay.toSeconds() + "s";
        }
        if (delay.toSecondsPart() == 0 && delay.toHoursPart() == 0 && delay.toDaysPart() == 0) {
            return delay.toMinutes() + "m";
        }
        if (delay.toSecondsPart() == 0 && delay.toMinutesPart() == 0 && delay.toDaysPart() == 0) {
            return delay.toHours() + "h";
        }
        return delay.toMillis() + "ms";
    }
}
