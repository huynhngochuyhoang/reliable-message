package io.github.huynhngochuyhoang.reliablemessage.rabbit.webflux.bridge;

import io.github.huynhngochuyhoang.reliablemessage.rabbit.webflux.bridge.autoconfigure.RabbitWebFluxBridgeAutoConfiguration;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.Assertions.assertThat;

class RabbitWebFluxBridgeAutoConfigurationTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(RabbitWebFluxBridgeAutoConfiguration.class));

    @Test
    void createsPropertiesBeanWhenEnabledByDefault() {
        contextRunner.run(context -> assertThat(context)
                .hasSingleBean(RabbitWebFluxBridgeProperties.class));
    }

    @Test
    void backsOffWhenBridgeIsDisabled() {
        contextRunner
                .withPropertyValues("message.reliability.rabbit.bridge.enabled=false")
                .run(context -> assertThat(context)
                        .doesNotHaveBean(RabbitWebFluxBridgeProperties.class));
    }
}
