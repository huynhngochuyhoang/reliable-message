package io.github.huynhngochuyhoang.reliablemessage.rpc.rabbit.webflux.bridge;

import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class RabbitRpcMetricsTest {

    @Test
    void countersIncludeRpcTagsAndExecutorMode() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RabbitRpcMetrics metrics = new RabbitRpcMetrics(registry, RabbitRpcExecutorMode.VIRTUAL_THREAD);

        metrics.request("orders.lookup");
        metrics.success("orders.lookup");
        metrics.failure("orders.lookup", "remote_error");
        metrics.timeout("orders.lookup");
        metrics.retry("orders.lookup");
        metrics.bulkheadRejected("orders.lookup");

        assertThat(counter(registry, "rpc_rabbit_requests_total", "orders.lookup", "request")).isEqualTo(1.0);
        assertThat(counter(registry, "rpc_rabbit_success_total", "orders.lookup", "success")).isEqualTo(1.0);
        assertThat(counter(registry, "rpc_rabbit_failed_total", "orders.lookup", "remote_error")).isEqualTo(1.0);
        assertThat(counter(registry, "rpc_rabbit_timeout_total", "orders.lookup", "timeout")).isEqualTo(1.0);
        assertThat(counter(registry, "rpc_rabbit_retry_total", "orders.lookup", "retry")).isEqualTo(1.0);
        assertThat(counter(registry, "rpc_rabbit_bulkhead_rejected_total", "orders.lookup", "bulkhead_rejected")).isEqualTo(1.0);
        assertThat(registry.find("rpc_rabbit_requests_total")
                .tag("runtime", "webflux")
                .tag("transport", "rabbit")
                .tag("rpc_client", "default")
                .tag("route", "orders.lookup")
                .tag("status", "request")
                .tag("executor_mode", "virtual-thread")
                .counter()).isNotNull();
    }

    @Test
    void durationMetersAreRpcSpecificAndStableAcrossRepeatedOperations() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RabbitRpcMetrics metrics = new RabbitRpcMetrics(registry, RabbitRpcExecutorMode.PLATFORM);

        Timer.Sample first = metrics.start();
        metrics.duration(first, "orders.lookup", "success");
        Timer.Sample second = metrics.start();
        metrics.duration(second, "orders.lookup", "success");

        assertThat(registry.find("rpc_rabbit_duration")
                .tag("runtime", "webflux")
                .tag("transport", "rabbit")
                .tag("route", "orders.lookup")
                .tag("status", "success")
                .tag("executor_mode", "platform")
                .timer()
                .count()).isEqualTo(2);
        assertThat(registry.find("rpc_rabbit_duration").timers()).hasSize(1);
        assertThat(registry.find("message_rabbit_bridge_publish_total").counter()).isNull();
    }

    @Test
    void metricsAvoidHighCardinalityTags() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RabbitRpcMetrics metrics = new RabbitRpcMetrics(registry, RabbitRpcExecutorMode.PLATFORM);

        metrics.request("orders.lookup");

        Set<String> tagKeys = registry.find("rpc_rabbit_requests_total").counter()
                .getId()
                .getTags()
                .stream()
                .map(tag -> tag.getKey())
                .collect(java.util.stream.Collectors.toSet());
        assertThat(tagKeys).doesNotContain("message_id", "correlation_id", "aggregate_id", "idempotency_key");
    }

    private static double counter(SimpleMeterRegistry registry, String name, String route, String status) {
        return registry.find(name)
                .tag("runtime", "webflux")
                .tag("transport", "rabbit")
                .tag("route", route)
                .tag("status", status)
                .counter()
                .count();
    }
}
