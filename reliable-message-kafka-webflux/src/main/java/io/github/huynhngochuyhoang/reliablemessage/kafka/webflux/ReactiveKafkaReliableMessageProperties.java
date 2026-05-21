package io.github.huynhngochuyhoang.reliablemessage.kafka.webflux;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@ConfigurationProperties(prefix = "message.reliability")
public class ReactiveKafkaReliableMessageProperties {

    private String runtime = "webflux";
    private String transport = "kafka";
    private String serviceName = "application";
    private final Kafka kafka = new Kafka();
    private final Reactive reactive = new Reactive();
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

    public Reactive getReactive() {
        return reactive;
    }

    public Retry getRetry() {
        return retry;
    }

    public Idempotency getIdempotency() {
        return idempotency;
    }

    public String topicName(String eventName) {
        return safeTopicPrefix() + Objects.requireNonNull(eventName, "eventName must not be null");
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
        private boolean listenerAutoStartup = true;
        private Map<String, Object> producerProperties = new HashMap<>();
        private Map<String, Object> consumerProperties = new HashMap<>();

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

        public boolean isListenerAutoStartup() {
            return listenerAutoStartup;
        }

        public void setListenerAutoStartup(boolean listenerAutoStartup) {
            this.listenerAutoStartup = listenerAutoStartup;
        }

        public Map<String, Object> getProducerProperties() {
            return producerProperties;
        }

        public void setProducerProperties(Map<String, Object> producerProperties) {
            this.producerProperties = producerProperties == null ? new HashMap<>() : new HashMap<>(producerProperties);
        }

        public Map<String, Object> getConsumerProperties() {
            return consumerProperties;
        }

        public void setConsumerProperties(Map<String, Object> consumerProperties) {
            this.consumerProperties = consumerProperties == null ? new HashMap<>() : new HashMap<>(consumerProperties);
        }
    }

    public static class Reactive {
        private int maxConcurrency = 64;
        private int prefetch = 256;

        public int getMaxConcurrency() {
            return maxConcurrency;
        }

        public void setMaxConcurrency(int maxConcurrency) {
            this.maxConcurrency = maxConcurrency;
        }

        public int getPrefetch() {
            return prefetch;
        }

        public void setPrefetch(int prefetch) {
            this.prefetch = prefetch;
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
