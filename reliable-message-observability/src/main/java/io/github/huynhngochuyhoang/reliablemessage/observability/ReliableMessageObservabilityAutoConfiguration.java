package io.github.huynhngochuyhoang.reliablemessage.observability;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.observation.ObservationRegistry;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

@AutoConfiguration
@ConditionalOnClass(MeterRegistry.class)
public class ReliableMessageObservabilityAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    MeterRegistry reliableMessageMeterRegistry() {
        return new SimpleMeterRegistry();
    }

    @Bean
    @ConditionalOnMissingBean
    MessageObservability reliableMessageObservability(
            MeterRegistry meterRegistry,
            ObjectProvider<ObservationRegistry> observationRegistry
    ) {
        return new MessageObservability(meterRegistry, observationRegistry.getIfAvailable(ObservationRegistry::create));
    }
}
