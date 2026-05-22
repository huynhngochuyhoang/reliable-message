package io.github.huynhngochuyhoang.reliablemessage.rabbit.webflux.bridge;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "message.reliability")
public class RabbitWebFluxBridgeProperties {

    private String runtime = "webflux";
    private String transport = "rabbit";
    private String mode = "blocking-bridge";
    private String serviceName = "application";
    private final Rabbit rabbit = new Rabbit();

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

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
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

    public String queueName(String eventName) {
        String safeServiceName = serviceName == null || serviceName.isBlank() ? "application" : serviceName;
        if (eventName == null || eventName.isBlank()) {
            throw new IllegalArgumentException("eventName must not be blank");
        }
        return safeServiceName + "." + eventName;
    }

    public enum ExecutorMode {
        PLATFORM,
        VIRTUAL_THREAD
    }

    public enum RejectionPolicy {
        FAIL_FAST
    }

    public static class Rabbit {
        private String exchange = "app.events";
        private boolean listenerAutoStartup = true;
        private final Bridge bridge = new Bridge();

        public String getExchange() {
            return exchange;
        }

        public void setExchange(String exchange) {
            this.exchange = exchange;
        }

        public boolean isListenerAutoStartup() {
            return listenerAutoStartup;
        }

        public void setListenerAutoStartup(boolean listenerAutoStartup) {
            this.listenerAutoStartup = listenerAutoStartup;
        }

        public Bridge getBridge() {
            return bridge;
        }
    }

    public static class Bridge {
        private boolean enabled = true;
        private ExecutorMode executorMode = ExecutorMode.PLATFORM;
        private int workerThreads = 16;
        private int queueCapacity = 10000;
        private int maxConcurrency = 256;
        private RejectionPolicy rejectionPolicy = RejectionPolicy.FAIL_FAST;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public ExecutorMode getExecutorMode() {
            return executorMode;
        }

        public void setExecutorMode(ExecutorMode executorMode) {
            this.executorMode = executorMode;
        }

        public int getWorkerThreads() {
            return workerThreads;
        }

        public void setWorkerThreads(int workerThreads) {
            if (workerThreads <= 0) {
                throw new IllegalArgumentException("workerThreads must be greater than 0");
            }
            this.workerThreads = workerThreads;
        }

        public int getQueueCapacity() {
            return queueCapacity;
        }

        public void setQueueCapacity(int queueCapacity) {
            if (queueCapacity < 0) {
                throw new IllegalArgumentException("queueCapacity must not be negative");
            }
            this.queueCapacity = queueCapacity;
        }

        public int getMaxConcurrency() {
            return maxConcurrency;
        }

        public void setMaxConcurrency(int maxConcurrency) {
            if (maxConcurrency <= 0) {
                throw new IllegalArgumentException("maxConcurrency must be greater than 0");
            }
            this.maxConcurrency = maxConcurrency;
        }

        public RejectionPolicy getRejectionPolicy() {
            return rejectionPolicy;
        }

        public void setRejectionPolicy(RejectionPolicy rejectionPolicy) {
            this.rejectionPolicy = rejectionPolicy;
        }
    }
}
