package io.github.huynhngochuyhoang.reliablemessage.rpc;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;

import java.util.Objects;

public class RpcMetrics {

    private final MeterRegistry meterRegistry;
    private final String prefix;

    public RpcMetrics(MeterRegistry meterRegistry, String prefix) {
        this.meterRegistry = Objects.requireNonNull(meterRegistry, "meterRegistry must not be null");
        this.prefix = prefix == null || prefix.isBlank() ? "rpc_client" : prefix;
    }

    public Timer.Sample start() {
        return Timer.start(meterRegistry);
    }

    public void request(String runtime, String transport, String status) {
        increment(prefix + "_requests_total", runtime, transport, status);
    }

    public void failure(String runtime, String transport, String status) {
        increment(prefix + "_failures_total", runtime, transport, status);
    }

    public void timeout(String runtime, String transport) {
        increment(prefix + "_timeout_total", runtime, transport, "timeout");
    }

    public void retry(String runtime, String transport) {
        increment(prefix + "_retry_total", runtime, transport, "retry");
    }

    public void duration(Timer.Sample sample, String runtime, String transport, String status) {
        if (sample != null) {
            sample.stop(Timer.builder(prefix + "_duration")
                    .tags(tags(runtime, transport, status))
                    .register(meterRegistry));
        }
    }

    private void increment(String name, String runtime, String transport, String status) {
        Counter.builder(name)
                .tags(tags(runtime, transport, status))
                .register(meterRegistry)
                .increment();
    }

    private static Tags tags(String runtime, String transport, String status) {
        return Tags.of(
                "runtime", value(runtime),
                "transport", value(transport),
                "status", value(status)
        );
    }

    private static String value(String value) {
        return value == null || value.isBlank() ? "unknown" : value;
    }
}
