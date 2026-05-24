package io.github.huynhngochuyhoang.reliablemessage.rabbit.webflux.bridge;

import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessage;
import io.github.huynhngochuyhoang.reliablemessage.core.serialization.MessageSerializer;
import io.github.huynhngochuyhoang.reliablemessage.rabbit.webflux.bridge.autoconfigure.RabbitWebFluxBridgeAutoConfiguration;
import io.github.huynhngochuyhoang.reliablemessage.webflux.IdempotencyStartResult;
import io.github.huynhngochuyhoang.reliablemessage.webflux.ReactiveIdempotencyStore;
import io.github.huynhngochuyhoang.reliablemessage.webflux.ReactiveReliablePublisher;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import reactor.core.publisher.Mono;

import java.lang.reflect.Proxy;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class RabbitWebFluxBridgeAutoConfigurationTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(RabbitWebFluxBridgeAutoConfiguration.class));

    @Test
    void createsPropertiesBeanWhenTransportIsUnset() {
        contextRunner.run(context -> assertThat(context)
                .hasSingleBean(RabbitWebFluxBridgeProperties.class));
    }

    @Test
    void createsPropertiesBeanWhenRabbitTransportIsEnabled() {
        contextRunner
                .withPropertyValues("message.reliability.transport=rabbit")
                .run(context -> assertThat(context)
                        .hasSingleBean(RabbitWebFluxBridgeProperties.class));
    }

    @Test
    void backsOffWhenBridgeIsDisabled() {
        contextRunner
                .withPropertyValues(
                        "message.reliability.transport=rabbit",
                        "message.reliability.rabbit.bridge.enabled=false"
                )
                .run(context -> assertThat(context)
                        .doesNotHaveBean(RabbitWebFluxBridgeProperties.class));
    }

    @Test
    void backsOffWhenTransportIsNotRabbit() {
        contextRunner
                .withPropertyValues("message.reliability.transport=kafka")
                .withBean(RabbitTemplate.class, RecordingRabbitTemplate::new)
                .withBean(MessageSerializer.class, RecordingSerializer::new)
                .run(context -> assertThat(context)
                        .doesNotHaveBean(RabbitWebFluxBridgeProperties.class)
                        .doesNotHaveBean(RabbitBridgeExecutorProvider.class)
                        .doesNotHaveBean(RabbitBridgeConcurrencyGuard.class)
                        .doesNotHaveBean(ReactiveRabbitBridgePublisher.class)
                        .doesNotHaveBean(ReactiveReliablePublisher.class));
    }

    @Test
    void wiresReactivePublisherWhenRabbitTemplateAndSerializerExist() {
        contextRunner
                .withPropertyValues("message.reliability.transport=rabbit")
                .withBean(RabbitTemplate.class, RecordingRabbitTemplate::new)
                .withBean(MessageSerializer.class, RecordingSerializer::new)
                .run(context -> assertThat(context)
                        .hasSingleBean(RabbitBridgeExecutorProvider.class)
                        .hasSingleBean(RabbitBridgeConcurrencyGuard.class)
                        .hasSingleBean(ReactiveRabbitBridgePublisher.class)
                        .hasSingleBean(ReactiveReliablePublisher.class));
    }

    @Test
    void wiresReactiveListenerRegistrarWhenConnectionFactoryAndSerializerExist() {
        contextRunner
                .withPropertyValues(
                        "message.reliability.transport=rabbit",
                        "message.reliability.rabbit.listener-auto-startup=false"
                )
                .withBean(ConnectionFactory.class, RabbitWebFluxBridgeAutoConfigurationTest::connectionFactory)
                .withBean(MessageSerializer.class, RecordingSerializer::new)
                .run(context -> assertThat(context)
                        .hasSingleBean(ReactiveRabbitBridgeListenerRegistrar.class));
    }

    @Test
    void failsFastWhenMultipleReactiveIdempotencyStoresAreAmbiguous() {
        contextRunner
                .withPropertyValues(
                        "message.reliability.transport=rabbit",
                        "message.reliability.rabbit.listener-auto-startup=false"
                )
                .withBean(ConnectionFactory.class, RabbitWebFluxBridgeAutoConfigurationTest::connectionFactory)
                .withBean(MessageSerializer.class, RecordingSerializer::new)
                .withBean("firstIdempotencyStore", ReactiveIdempotencyStore.class, RecordingIdempotencyStore::new)
                .withBean("secondIdempotencyStore", ReactiveIdempotencyStore.class, RecordingIdempotencyStore::new)
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure()).hasMessageContaining("ReactiveIdempotencyStore");
                });
    }

    @Test
    void failsFastWhenMultipleFailureHandlersAreAmbiguous() {
        contextRunner
                .withPropertyValues(
                        "message.reliability.transport=rabbit",
                        "message.reliability.rabbit.listener-auto-startup=false"
                )
                .withBean(ConnectionFactory.class, RabbitWebFluxBridgeAutoConfigurationTest::connectionFactory)
                .withBean(MessageSerializer.class, RecordingSerializer::new)
                .withBean("firstFailureHandler", ReactiveRabbitBridgeFailureHandler.class, () -> RecordingFailureHandler::handle)
                .withBean("secondFailureHandler", ReactiveRabbitBridgeFailureHandler.class, () -> RecordingFailureHandler::handle)
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure()).hasMessageContaining("ReactiveRabbitBridgeFailureHandler");
                });
    }

    @Test
    void doesNotCreateRabbitAdminWhenOnlyConnectionFactoryExists() {
        contextRunner
                .withPropertyValues("message.reliability.transport=rabbit")
                .withBean(ConnectionFactory.class, RabbitWebFluxBridgeAutoConfigurationTest::connectionFactory)
                .run(context -> assertThat(context)
                        .doesNotHaveBean(RabbitAdmin.class)
                        .hasSingleBean(ReactiveRabbitBridgeTopologyAutoConfigurer.class));
    }

    @Test
    void wiresTopologyAutoConfigurerWithUserProvidedRabbitAdmin() {
        contextRunner
                .withPropertyValues("message.reliability.transport=rabbit")
                .withBean(ConnectionFactory.class, RabbitWebFluxBridgeAutoConfigurationTest::connectionFactory)
                .withBean(RabbitAdmin.class, () -> new RabbitAdmin(connectionFactory()))
                .run(context -> assertThat(context)
                        .hasSingleBean(RabbitAdmin.class)
                        .hasSingleBean(ReactiveRabbitBridgeTopologyAutoConfigurer.class));
    }

    private static ConnectionFactory connectionFactory() {
        return (ConnectionFactory) Proxy.newProxyInstance(
                ConnectionFactory.class.getClassLoader(),
                new Class<?>[]{ConnectionFactory.class},
                (proxy, method, args) -> null
        );
    }

    private static final class RecordingRabbitTemplate extends RabbitTemplate {
        @Override
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

    private static final class RecordingIdempotencyStore implements ReactiveIdempotencyStore {
        @Override
        public Mono<IdempotencyStartResult> tryStart(String key, Duration ttl) {
            return Mono.just(IdempotencyStartResult.startAccepted());
        }

        @Override
        public Mono<Void> markSuccess(String key) {
            return Mono.empty();
        }

        @Override
        public Mono<Void> markFailed(String key, Throwable error) {
            return Mono.empty();
        }
    }

    private static final class RecordingFailureHandler {
        private static void handle(
                ReactiveRabbitBridgeListenerEndpoint endpoint,
                ReliableMessage<?> reliableMessage,
                org.springframework.amqp.core.Message amqpMessage,
                Throwable error
        ) {
        }
    }
}
