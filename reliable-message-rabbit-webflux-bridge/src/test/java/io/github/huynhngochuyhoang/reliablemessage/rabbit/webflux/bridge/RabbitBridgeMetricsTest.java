package io.github.huynhngochuyhoang.reliablemessage.rabbit.webflux.bridge;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.concurrent.*;

import static org.assertj.core.api.Assertions.assertThat;

class RabbitBridgeMetricsTest {

    @Test
    void publishSuccessCounterIncludesBridgeTags() {
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        RabbitBridgeMetrics metrics = new RabbitBridgeMetrics(meterRegistry, RabbitWebFluxBridgeProperties.ExecutorMode.PLATFORM);

        metrics.publish("order.created", "success");

        assertThat(counter(meterRegistry, "message_rabbit_bridge_publish_total", "order.created", "success")).isEqualTo(1.0);
        assertThat(meterRegistry.find("message_rabbit_bridge_publish_total")
                .tag("runtime", "webflux-bridge")
                .tag("transport", "rabbit")
                .tag("executor_mode", "platform")
                .tag("event_name", "order.created")
                .tag("status", "success")
                .counter()).isNotNull();
    }

    @Test
    void publishFailureCounterIncrements() {
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        RabbitBridgeMetrics metrics = new RabbitBridgeMetrics(meterRegistry, RabbitWebFluxBridgeProperties.ExecutorMode.PLATFORM);

        metrics.publish("order.created", "failure");

        assertThat(counter(meterRegistry, "message_rabbit_bridge_publish_total", "order.created", "failure")).isEqualTo(1.0);
    }

    @Test
    void consumeSuccessFailureAndDuplicateCountersIncrement() {
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        RabbitBridgeMetrics metrics = new RabbitBridgeMetrics(meterRegistry, RabbitWebFluxBridgeProperties.ExecutorMode.VIRTUAL_THREAD);

        metrics.consume("order.created", "success");
        metrics.consume("order.created", "failure");
        metrics.duplicate("order.created", "success");

        assertThat(counter(meterRegistry, "message_rabbit_bridge_consume_total", "order.created", "success")).isEqualTo(1.0);
        assertThat(counter(meterRegistry, "message_rabbit_bridge_consume_total", "order.created", "failure")).isEqualTo(1.0);
        assertThat(counter(meterRegistry, "message_rabbit_bridge_duplicate_total", "order.created", "success")).isEqualTo(1.0);
        assertThat(meterRegistry.find("message_rabbit_bridge_consume_total")
                .tag("executor_mode", "virtual-thread")
                .tag("event_name", "order.created")
                .tag("status", "success")
                .counter()).isNotNull();
    }

    @Test
    void executorRejectionCounterIncrements() {
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        RabbitBridgeMetrics metrics = new RabbitBridgeMetrics(meterRegistry, RabbitWebFluxBridgeProperties.ExecutorMode.PLATFORM);

        metrics.executorRejected("order.created");

        assertThat(counter(meterRegistry, "message_rabbit_bridge_executor_rejected_total", "order.created", "rejected")).isEqualTo(1.0);
    }

    @Test
    void failureOutcomeCountersIncrement() {
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        RabbitBridgeMetrics metrics = new RabbitBridgeMetrics(meterRegistry, RabbitWebFluxBridgeProperties.ExecutorMode.PLATFORM);

        metrics.failureOutcome("order.created", "retry");
        metrics.failureOutcome("order.created", "dlq");

        assertThat(counter(meterRegistry, "message_rabbit_bridge_failure_outcome_total", "order.created", "retry")).isEqualTo(1.0);
        assertThat(counter(meterRegistry, "message_rabbit_bridge_failure_outcome_total", "order.created", "dlq")).isEqualTo(1.0);
        assertThat(meterRegistry.find("message_rabbit_bridge_failure_outcome_total")
                .tag("runtime", "webflux-bridge")
                .tag("transport", "rabbit")
                .tag("executor_mode", "platform")
                .tag("event_name", "order.created")
                .tag("status", "retry")
                .counter()).isNotNull();
    }

    @Test
    void exposesPlatformExecutorActiveAndQueuedGaugesWhenAvailable() {
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        RabbitBridgeMetrics metrics = new RabbitBridgeMetrics(meterRegistry, RabbitWebFluxBridgeProperties.ExecutorMode.PLATFORM);
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                1,
                1,
                0L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(1)
        );
        try {
            metrics.bindExecutor(executor);

            assertThat(meterRegistry.find("message_rabbit_bridge_executor_active")
                    .tag("runtime", "webflux-bridge")
                    .tag("transport", "rabbit")
                    .tag("executor_mode", "platform")
                    .tag("event_name", "all")
                    .tag("status", "active")
                    .gauge()).isNotNull();
            assertThat(meterRegistry.find("message_rabbit_bridge_executor_queued")
                    .tag("status", "queued")
                    .gauge()).isNotNull();
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void virtualThreadModeDoesNotExposePlatformQueueGauges() {
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        RabbitBridgeMetrics metrics = new RabbitBridgeMetrics(meterRegistry, RabbitWebFluxBridgeProperties.ExecutorMode.VIRTUAL_THREAD);
        ExecutorService executor = new AbstractExecutorService() {
            private volatile boolean shutdown;


            public void shutdown() {
                shutdown = true;
            }


            public java.util.List<Runnable> shutdownNow() {
                shutdown = true;
                return java.util.List.of();
            }


            public boolean isShutdown() {
                return shutdown;
            }


            public boolean isTerminated() {
                return shutdown;
            }


            public boolean awaitTermination(long timeout, TimeUnit unit) {
                return shutdown;
            }


            public void execute(Runnable command) {
                command.run();
            }
        };

        metrics.bindExecutor(executor);

        assertThat(meterRegistry.find("message_rabbit_bridge_executor_active").gauge()).isNull();
        assertThat(meterRegistry.find("message_rabbit_bridge_executor_queued").gauge()).isNull();
    }

    @Test
    void repeatedOperationsReuseStableMetersAndAvoidHighCardinalityTags() {
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        RabbitBridgeMetrics metrics = new RabbitBridgeMetrics(meterRegistry, RabbitWebFluxBridgeProperties.ExecutorMode.PLATFORM);

        metrics.publish("order.created", "success");
        metrics.publish("order.created", "success");

        assertThat(counter(meterRegistry, "message_rabbit_bridge_publish_total", "order.created", "success")).isEqualTo(2.0);
        assertThat(meterRegistry.find("message_rabbit_bridge_publish_total").counters()).hasSize(1);
        Set<String> tagKeys = meterRegistry.find("message_rabbit_bridge_publish_total").counter()
                .getId()
                .getTags()
                .stream()
                .map(tag -> tag.getKey())
                .collect(java.util.stream.Collectors.toSet());
        assertThat(tagKeys).doesNotContain("message_id", "correlation_id", "aggregate_id", "idempotency_key");
    }

    private static double counter(SimpleMeterRegistry meterRegistry, String name, String eventName, String status) {
        return meterRegistry.find(name)
                .tag("event_name", eventName)
                .tag("status", status)
                .counter()
                .count();
    }
}
