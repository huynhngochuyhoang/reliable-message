package io.github.huynhngochuyhoang.reliablemessage.observability;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;

import java.util.Objects;
import java.util.function.Supplier;

public class MessageObservability {

    private final MeterRegistry meterRegistry;
    private final ObservationRegistry observationRegistry;

    public MessageObservability(MeterRegistry meterRegistry, ObservationRegistry observationRegistry) {
        this.meterRegistry = Objects.requireNonNull(meterRegistry, "meterRegistry must not be null");
        this.observationRegistry = observationRegistry == null ? ObservationRegistry.NOOP : observationRegistry;
    }

    public void increment(String metricName, MessageTags tags) {
        Counter.builder(metricName)
                .tags(tags.toMicrometerTags())
                .register(meterRegistry)
                .increment();
    }

    public void observe(String spanName, String durationMetricName, MessageTags tags, Runnable action) {
        observe(spanName, durationMetricName, tags, () -> {
            action.run();
            return null;
        });
    }

    public <T> T observe(String spanName, String durationMetricName, MessageTags tags, Supplier<T> action) {
        Timer.Sample sample = durationMetricName == null ? null : Timer.start(meterRegistry);
        try {
            return observation(spanName, tags).observe(action);
        } finally {
            if (sample != null) {
                sample.stop(Timer.builder(durationMetricName)
                        .tags(tags.toMicrometerTags())
                        .register(meterRegistry));
            }
        }
    }

    private Observation observation(String spanName, MessageTags tags) {
        Observation observation = Observation.createNotStarted(spanName, observationRegistry)
                .lowCardinalityKeyValue("runtime", value(tags.runtime()))
                .lowCardinalityKeyValue("transport", value(tags.transport()))
                .lowCardinalityKeyValue("event_name", value(tags.eventName()));
        if (tags.consumer() != null && !tags.consumer().isBlank()) {
            observation = observation.lowCardinalityKeyValue("consumer", tags.consumer());
        }
        if (tags.status() != null && !tags.status().isBlank()) {
            observation = observation.lowCardinalityKeyValue("status", tags.status());
        }
        return observation;
    }

    private static String value(String value) {
        return value == null || value.isBlank() ? "unknown" : value;
    }
}
