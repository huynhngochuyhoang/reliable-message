package io.github.huynhngochuyhoang.reliablemessage.rabbit.webflux.bridge;

import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessage;
import io.github.huynhngochuyhoang.reliablemessage.core.serialization.MessageSerializer;
import io.github.huynhngochuyhoang.reliablemessage.rabbit.webflux.bridge.autoconfigure.RabbitWebFluxBridgeAutoConfiguration;
import io.github.huynhngochuyhoang.reliablemessage.webflux.ReactiveReliableListener;
import io.github.huynhngochuyhoang.reliablemessage.webflux.ReactiveReliablePublisher;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Mono;

import java.lang.reflect.Proxy;

import static org.assertj.core.api.Assertions.assertThat;

class RabbitWebFluxBridgeSampleContextSmokeTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(RabbitWebFluxBridgeAutoConfiguration.class))
            .withUserConfiguration(SampleConfig.class)
            .withPropertyValues(
                    "message.reliability.transport=rabbit",
                    "message.reliability.service-name=orders",
                    "message.reliability.rabbit.listener-auto-startup=false"
            );

    @Test
    void rabbitWebFluxBridgeSampleContextLoadsWithoutRpcBeans() {
        contextRunner.run(context -> {
            assertThat(context)
                    .hasSingleBean(RabbitWebFluxBridgeProperties.class)
                    .hasSingleBean(RabbitBridgeExecutorProvider.class)
                    .hasSingleBean(RabbitBridgeConcurrencyGuard.class)
                    .hasSingleBean(ReactiveReliablePublisher.class)
                    .hasSingleBean(ReactiveRabbitBridgeListenerRegistrar.class)
                    .doesNotHaveBean("reactiveRabbitRpcClient")
                    .doesNotHaveBean("rabbitRpcBridgeExecutorProvider");
            assertThat(context.getEnvironment().getProperty("message.reliability.outbox.schema.payload-storage"))
                    .isNull();
        });
    }

    @Configuration(proxyBeanMethods = false)
    static class SampleConfig {

        @Bean
        ConnectionFactory connectionFactory() {
            return (ConnectionFactory) Proxy.newProxyInstance(
                    ConnectionFactory.class.getClassLoader(),
                    new Class<?>[]{ConnectionFactory.class},
                    (proxy, method, args) -> null
            );
        }

        @Bean
        RabbitTemplate rabbitTemplate() {
            return new NonStartingRabbitTemplate();
        }

        @Bean
        MessageSerializer messageSerializer() {
            return new RecordingSerializer();
        }

        @Bean
        SampleListener sampleListener() {
            return new SampleListener();
        }
    }

    static final class SampleListener {

        @ReactiveReliableListener("order.created")
        Mono<Void> onOrderCreated(ReliableMessage<OrderCreated> message) {
            return Mono.empty();
        }
    }

    record OrderCreated(String orderId) {
    }

    private static final class RecordingSerializer implements MessageSerializer {
        @Override
        public <T> byte[] serialize(ReliableMessage<T> message) {
            return new byte[]{1};
        }

        @Override
        public <T> ReliableMessage<T> deserialize(byte[] content, Class<T> payloadType) {
            throw new UnsupportedOperationException("deserialize is not used by smoke tests");
        }
    }

    private static final class NonStartingRabbitTemplate extends RabbitTemplate {
        @Override
        public void afterPropertiesSet() {
        }
    }
}
