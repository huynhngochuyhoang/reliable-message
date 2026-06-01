package io.github.huynhngochuyhoang.reliablemessage.rpc.rabbit.webflux.bridge;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;

public class RabbitRpcMetrics {

    private final MeterRegistry meterRegistry;
    private final String executorMode;

    public RabbitRpcMetrics(MeterRegistry meterRegistry, RabbitRpcExecutorMode executorMode) {
        this.meterRegistry = meterRegistry;
        this.executorMode = executorMode == null ? "unknown" : executorMode.name().toLowerCase().replace("_", "-");
    }

    public static RabbitRpcMetrics noop(RabbitRpcExecutorMode executorMode) {
        return new RabbitRpcMetrics(null, executorMode);
    }

    public Timer.Sample start() {
        return meterRegistry == null ? null : Timer.start(meterRegistry);
    }

    public void request(String route) {
        increment("rpc_rabbit_requests_total", route, "request");
    }

    public void success(String route) {
        increment("rpc_rabbit_success_total", route, "success");
    }

    public void failure(String route, String status) {
        increment("rpc_rabbit_failed_total", route, status);
    }

    public void timeout(String route) {
        increment("rpc_rabbit_timeout_total", route, "timeout");
    }

    public void retry(String route) {
        increment("rpc_rabbit_retry_total", route, "retry");
    }

    public void bulkheadRejected(String route) {
        increment("rpc_rabbit_bulkhead_rejected_total", route, "bulkhead_rejected");
    }

    public void duration(Timer.Sample sample, String route, String status) {
        if (meterRegistry != null && sample != null) {
            record(() -> sample.stop(Timer.builder("rpc_rabbit_duration")
                    .tags(tags(route, status))
                    .register(meterRegistry)));
        }
    }

    private void increment(String name, String route, String status) {
        if (meterRegistry == null) {
            return;
        }
        record(() -> Counter.builder(name)
                .tags(tags(route, status))
                .register(meterRegistry)
                .increment());
    }

    private static void record(Runnable recorder) {
        try {
            recorder.run();
        } catch (RuntimeException ignored) {
            // Metrics are best-effort and must not affect RPC business flow.
        }
    }

    private Tags tags(String route, String status) {
        return Tags.of(
                "runtime", "webflux",
                "transport", "rabbit",
                "rpc_client", "default",
                "route", value(route),
                "status", value(status),
                "executor_mode", executorMode
        );
    }

    private static String value(String value) {
        return value == null || value.isBlank() ? "unknown" : value;
    }
}
