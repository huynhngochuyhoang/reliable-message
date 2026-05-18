package io.github.huynhngochuyhoang.reliablemessage.kafka.mvc;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@ConfigurationProperties(prefix = "message.reliability")
public class KafkaReliableMessageProperties {

    private String runtime = "mvc";
    private String transport = "kafka";
    private String serviceName = "application";
    private final Kafka kafka = new Kafka();
    private final Retry retry = new Retry();
    private final Idempotency idempotency = new Idempotency();

    public String getRuntime() {
        return runtime;
    }

    public void setRuntime(String runtime) {
        this.runtime = runtime;
    }

    public String getTransport() {
        return transport;
    }

    public void setTransport(String transport) {
        this.transport = transport;
    }

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    public Kafka getKafka() {
        return kafka;
    }

    public Retry getRetry() {
        return retry;
    }

    public Idempotency getIdempotency() {
        return idempotency;
    }

    public String topicName(String eventName) {
        String safeEventName = Objects.requireNonNull(eventName, "eventName must not be null");
        return safeTopicPrefix() + safeEventName;
    }

    public String consumerGroup() {
        String group = kafka.getConsumerGroup();
        if (group != null && !group.isBlank()) {
            return group;
        }
        return serviceName == null || serviceName.isBlank() ? "application" : serviceName;
    }

    private String safeTopicPrefix() {
        return kafka.getTopicPrefix() == null ? "" : kafka.getTopicPrefix();
    }

    public static class Kafka {
        private String topicPrefix = "";
        private String consumerGroup;
        private boolean autoDeclare = true;
        private boolean listenerAutoStartup = true;
        private int partitions = 1;
        private short replicationFactor = 1;
        private Duration publishTimeout = Duration.ofSeconds(5);

        public String getTopicPrefix() {
            return topicPrefix;
        }

        public void setTopicPrefix(String topicPrefix) {
            this.topicPrefix = topicPrefix;
        }

        public String getConsumerGroup() {
            return consumerGroup;
        }

        public void setConsumerGroup(String consumerGroup) {
            this.consumerGroup = consumerGroup;
        }

        public boolean isAutoDeclare() {
            return autoDeclare;
        }

        public void setAutoDeclare(boolean autoDeclare) {
            this.autoDeclare = autoDeclare;
        }

        public boolean isListenerAutoStartup() {
            return listenerAutoStartup;
        }

        public void setListenerAutoStartup(boolean listenerAutoStartup) {
            this.listenerAutoStartup = listenerAutoStartup;
        }

        public int getPartitions() {
            return partitions;
        }

        public void setPartitions(int partitions) {
            this.partitions = partitions;
        }

        public short getReplicationFactor() {
            return replicationFactor;
        }

        public void setReplicationFactor(short replicationFactor) {
            this.replicationFactor = replicationFactor;
        }

        public Duration getPublishTimeout() {
            return publishTimeout;
        }

        public void setPublishTimeout(Duration publishTimeout) {
            this.publishTimeout = publishTimeout;
        }
    }

    public static class Retry {
        private int attempts = 5;
        private List<Duration> backoff = new ArrayList<>(List.of(
                Duration.ofSeconds(5),
                Duration.ofSeconds(30),
                Duration.ofMinutes(1),
                Duration.ofMinutes(5)
        ));

        public int getAttempts() {
            return attempts;
        }

        public void setAttempts(int attempts) {
            this.attempts = attempts;
        }

        public List<Duration> getBackoff() {
            return List.copyOf(backoff);
        }

        public void setBackoff(List<Duration> backoff) {
            this.backoff = backoff == null ? new ArrayList<>() : new ArrayList<>(backoff);
        }
    }

    public static class Idempotency {
        private Duration ttl = Duration.ofHours(24);

        public Duration getTtl() {
            return ttl;
        }

        public void setTtl(Duration ttl) {
            this.ttl = ttl;
        }
    }
}
