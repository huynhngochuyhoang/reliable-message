package io.github.huynhngochuyhoang.reliablemessage.rabbit.webflux.bridge;

import io.github.huynhngochuyhoang.reliablemessage.rabbit.webflux.bridge.autoconfigure.RabbitWebFluxBridgeAutoConfiguration;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.Assertions.assertThat;

class RabbitWebFluxBridgePropertiesTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(RabbitWebFluxBridgeAutoConfiguration.class));

    @Test
    void bindsBridgeProperties() {
        contextRunner
                .withPropertyValues(
                        "message.reliability.runtime=webflux",
                        "message.reliability.transport=rabbit",
                        "message.reliability.mode=blocking-bridge",
                        "message.reliability.rabbit.exchange=orders.events",
                        "message.reliability.rabbit.bridge.executor-mode=virtual-thread",
                        "message.reliability.rabbit.bridge.worker-threads=8",
                        "message.reliability.rabbit.bridge.queue-capacity=128",
                        "message.reliability.rabbit.bridge.max-concurrency=64",
                        "message.reliability.rabbit.bridge.rejection-policy=fail-fast"
                )
                .run(context -> {
                    RabbitWebFluxBridgeProperties properties = context.getBean(RabbitWebFluxBridgeProperties.class);

                    assertThat(properties.getRuntime()).isEqualTo("webflux");
                    assertThat(properties.getTransport()).isEqualTo("rabbit");
                    assertThat(properties.getMode()).isEqualTo("blocking-bridge");
                    assertThat(properties.getRabbit().getExchange()).isEqualTo("orders.events");
                    assertThat(properties.getRabbit().getBridge().getExecutorMode())
                            .isEqualTo(RabbitWebFluxBridgeProperties.ExecutorMode.VIRTUAL_THREAD);
                    assertThat(properties.getRabbit().getBridge().getWorkerThreads()).isEqualTo(8);
                    assertThat(properties.getRabbit().getBridge().getQueueCapacity()).isEqualTo(128);
                    assertThat(properties.getRabbit().getBridge().getMaxConcurrency()).isEqualTo(64);
                    assertThat(properties.getRabbit().getBridge().getRejectionPolicy())
                            .isEqualTo(RabbitWebFluxBridgeProperties.RejectionPolicy.FAIL_FAST);
                });
    }

    @Test
    void usesBoundedDefaults() {
        contextRunner.run(context -> {
            RabbitWebFluxBridgeProperties.Bridge bridge =
                    context.getBean(RabbitWebFluxBridgeProperties.class).getRabbit().getBridge();

            assertThat(bridge.isEnabled()).isTrue();
            assertThat(bridge.getExecutorMode()).isEqualTo(RabbitWebFluxBridgeProperties.ExecutorMode.PLATFORM);
            assertThat(bridge.getWorkerThreads()).isGreaterThan(0);
            assertThat(bridge.getQueueCapacity()).isGreaterThan(0);
            assertThat(bridge.getMaxConcurrency()).isGreaterThan(0);
            assertThat(bridge.getRejectionPolicy()).isEqualTo(RabbitWebFluxBridgeProperties.RejectionPolicy.FAIL_FAST);
        });
    }

    @Test
    void rejectsInvalidMaxConcurrency() {
        contextRunner
                .withPropertyValues("message.reliability.rabbit.bridge.max-concurrency=0")
                .run(context -> assertThat(context).hasFailed());
    }

    @Test
    void rejectsNegativeQueueCapacity() {
        contextRunner
                .withPropertyValues("message.reliability.rabbit.bridge.queue-capacity=-1")
                .run(context -> assertThat(context).hasFailed());
    }

    @Test
    void rejectsInvalidWorkerThreads() {
        contextRunner
                .withPropertyValues("message.reliability.rabbit.bridge.worker-threads=0")
                .run(context -> assertThat(context).hasFailed());
    }
}
