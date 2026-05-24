package io.github.huynhngochuyhoang.reliablemessage.rabbit.webflux.bridge;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;

import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

public class RabbitBridgeMetrics {

    private static final String RUNTIME = "webflux-bridge";
    private static final String TRANSPORT = "rabbit";
    private static final String PUBLISH_TOTAL = "message_rabbit_bridge_publish_total";
    private static final String CONSUME_TOTAL = "message_rabbit_bridge_consume_total";
    private static final String DUPLICATE_TOTAL = "message_rabbit_bridge_duplicate_total";
    private static final String FAILURE_OUTCOME_TOTAL = "message_rabbit_bridge_failure_outcome_total";
    private static final String EXECUTOR_REJECTED_TOTAL = "message_rabbit_bridge_executor_rejected_total";
    private static final String EXECUTOR_ACTIVE = "message_rabbit_bridge_executor_active";
    private static final String EXECUTOR_QUEUED = "message_rabbit_bridge_executor_queued";

    private final MeterRegistry meterRegistry;
    private final String executorMode;

    public RabbitBridgeMetrics(MeterRegistry meterRegistry, RabbitWebFluxBridgeProperties.ExecutorMode executorMode) {
        this(Objects.requireNonNull(meterRegistry, "meterRegistry must not be null"), executorMode, false);
    }

    private RabbitBridgeMetrics(
            MeterRegistry meterRegistry,
            RabbitWebFluxBridgeProperties.ExecutorMode executorMode,
            boolean noop
    ) {
        this.meterRegistry = meterRegistry;
        this.executorMode = executorModeName(executorMode);
    }

    public static RabbitBridgeMetrics noop() {
        return new RabbitBridgeMetrics(null, RabbitWebFluxBridgeProperties.ExecutorMode.PLATFORM, true);
    }

    public void publish(String eventName, String status) {
        increment(PUBLISH_TOTAL, eventName, status);
    }

    public void consume(String eventName, String status) {
        increment(CONSUME_TOTAL, eventName, status);
    }

    public void duplicate(String eventName, String status) {
        increment(DUPLICATE_TOTAL, eventName, status);
    }

    public void failureOutcome(String eventName, String status) {
        increment(FAILURE_OUTCOME_TOTAL, eventName, status);
    }

    public void executorRejected(String eventName) {
        increment(EXECUTOR_REJECTED_TOTAL, eventName, "rejected");
    }

    public void bindExecutor(ExecutorService executor) {
        if (meterRegistry == null || !(executor instanceof ThreadPoolExecutor threadPoolExecutor)) {
            return;
        }
        record(() -> {
            Gauge.builder(EXECUTOR_ACTIVE, threadPoolExecutor, ThreadPoolExecutor::getActiveCount)
                    .tags(tags("all", "active"))
                    .register(meterRegistry);
            Gauge.builder(EXECUTOR_QUEUED, threadPoolExecutor, value -> value.getQueue().size())
                    .tags(tags("all", "queued"))
                    .register(meterRegistry);
        });
    }

    private void increment(String metricName, String eventName, String status) {
        if (meterRegistry == null) {
            return;
        }
        record(() -> Counter.builder(metricName)
                .tags(tags(eventName, status))
                .register(meterRegistry)
                .increment());
    }

    private Tags tags(String eventName, String status) {
        return Tags.of(
                "runtime", RUNTIME,
                "transport", TRANSPORT,
                "executor_mode", executorMode,
                "event_name", value(eventName),
                "status", value(status)
        );
    }

    private static void record(Runnable recorder) {
        try {
            recorder.run();
        } catch (RuntimeException ignored) {
            // Metrics must not change bridge business flow.
        }
    }

    private static String executorModeName(RabbitWebFluxBridgeProperties.ExecutorMode executorMode) {
        if (executorMode == RabbitWebFluxBridgeProperties.ExecutorMode.VIRTUAL_THREAD) {
            return "virtual-thread";
        }
        return "platform";
    }

    private static String value(String value) {
        return value == null || value.isBlank() ? "unknown" : value;
    }
}
