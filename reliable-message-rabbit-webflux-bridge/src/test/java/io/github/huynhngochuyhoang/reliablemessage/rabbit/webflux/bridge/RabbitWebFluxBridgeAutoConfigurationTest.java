package io.github.huynhngochuyhoang.reliablemessage.rabbit.webflux.bridge;

import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessage;
import io.github.huynhngochuyhoang.reliablemessage.core.serialization.MessageSerializer;
import io.github.huynhngochuyhoang.reliablemessage.rabbit.webflux.bridge.autoconfigure.RabbitWebFluxBridgeAutoConfiguration;
import io.github.huynhngochuyhoang.reliablemessage.webflux.ReactiveReliablePublisher;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
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

    @Test
    void wiresReactivePublisherWhenRabbitTemplateAndSerializerExist() {
        contextRunner
                .withBean(RabbitTemplate.class, RecordingRabbitTemplate::new)
                .withBean(MessageSerializer.class, RecordingSerializer::new)
                .run(context -> assertThat(context)
                        .hasSingleBean(RabbitBridgeExecutorProvider.class)
                        .hasSingleBean(RabbitBridgeConcurrencyGuard.class)
                        .hasSingleBean(ReactiveRabbitBridgePublisher.class)
                        .hasSingleBean(ReactiveReliablePublisher.class));
    }

    private static final class RecordingRabbitTemplate extends RabbitTemplate {
        
        public void afterPropertiesSet() {
        }
    }

    private static final class RecordingSerializer implements MessageSerializer {
        @Override
        public <T> byte[] serialize(ReliableMessage<T> message) {
            return new byte[]{1};
        }

        @Override
        public <T> ReliableMessage<T> deserialize(byte[] content, Class<T> payloadType) {
            throw new UnsupportedOperationException("deserialize is not used by auto-configuration tests");
        }
    }
}
