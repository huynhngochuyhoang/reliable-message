package io.github.huynhngochuyhoang.reliablemessage.rabbit.mvc;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.Objects;

@ConfigurationProperties(prefix = "message.reliability")
public class RabbitReliableMessageProperties {

    private String runtime = "mvc";
    private String transport = "rabbit";
    private String serviceName = "application";
    private final Rabbit rabbit = new Rabbit();
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

    public Rabbit getRabbit() {
        return rabbit;
    }

    public Idempotency getIdempotency() {
        return idempotency;
    }

    public String queueName(String eventName) {
        return safeServiceName() + "." + Objects.requireNonNull(eventName, "eventName must not be null");
    }

    private String safeServiceName() {
        return serviceName == null || serviceName.isBlank() ? "application" : serviceName;
    }

    public static class Rabbit {
        private String exchange = "app.events";
        private boolean autoDeclare = true;
        private boolean publisherConfirm = true;
        private Duration publisherConfirmTimeout = Duration.ofSeconds(5);
        private boolean listenerAutoStartup = true;

        public String getExchange() {
            return exchange;
        }

        public void setExchange(String exchange) {
            this.exchange = exchange;
        }

        public boolean isAutoDeclare() {
            return autoDeclare;
        }

        public void setAutoDeclare(boolean autoDeclare) {
            this.autoDeclare = autoDeclare;
        }

        public boolean isPublisherConfirm() {
            return publisherConfirm;
        }

        public void setPublisherConfirm(boolean publisherConfirm) {
            this.publisherConfirm = publisherConfirm;
        }

        public Duration getPublisherConfirmTimeout() {
            return publisherConfirmTimeout;
        }

        public void setPublisherConfirmTimeout(Duration publisherConfirmTimeout) {
            this.publisherConfirmTimeout = publisherConfirmTimeout;
        }

        public boolean isListenerAutoStartup() {
            return listenerAutoStartup;
        }

        public void setListenerAutoStartup(boolean listenerAutoStartup) {
            this.listenerAutoStartup = listenerAutoStartup;
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
