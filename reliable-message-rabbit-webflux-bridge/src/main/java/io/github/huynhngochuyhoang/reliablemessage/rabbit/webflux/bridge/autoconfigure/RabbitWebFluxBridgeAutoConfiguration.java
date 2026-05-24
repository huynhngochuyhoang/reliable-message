package io.github.huynhngochuyhoang.reliablemessage.rabbit.webflux.bridge.autoconfigure;

import io.github.huynhngochuyhoang.reliablemessage.core.serialization.MessageSerializer;
import io.github.huynhngochuyhoang.reliablemessage.rabbit.webflux.bridge.*;
import io.github.huynhngochuyhoang.reliablemessage.webflux.ReactiveIdempotencyStore;
import io.github.huynhngochuyhoang.reliablemessage.webflux.ReactiveReliablePublisher;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import java.time.Clock;

@AutoConfiguration
@ConditionalOnClass(RabbitTemplate.class)
@ConditionalOnProperty(prefix = "message.reliability", name = "transport", havingValue = "rabbit", matchIfMissing = true)
@ConditionalOnProperty(prefix = "message.reliability.rabbit.bridge", name = "enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(RabbitWebFluxBridgeProperties.class)
public class RabbitWebFluxBridgeAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    Clock reliableMessageRabbitBridgeClock() {
        return Clock.systemUTC();
    }

    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean
    RabbitBridgeExecutorProvider rabbitBridgeExecutorProvider(RabbitWebFluxBridgeProperties properties) {
        RabbitWebFluxBridgeProperties.Bridge bridge = properties.getRabbit().getBridge();
        return switch (bridge.getExecutorMode()) {
            case PLATFORM -> new PlatformThreadRabbitBridgeExecutorProvider(bridge);
            case VIRTUAL_THREAD -> new VirtualThreadRabbitBridgeExecutorProvider(bridge);
        };
    }

    @Bean
    @ConditionalOnMissingBean
    RabbitBridgeConcurrencyGuard rabbitBridgeConcurrencyGuard(RabbitWebFluxBridgeProperties properties) {
        return new RabbitBridgeConcurrencyGuard(properties.getRabbit().getBridge());
    }

    @Bean
    @ConditionalOnMissingBean
    ReactiveRabbitBridgeTopologyAutoConfigurer reactiveRabbitBridgeTopologyAutoConfigurer(
            ObjectProvider<RabbitAdmin> rabbitAdmin,
            RabbitWebFluxBridgeProperties properties
    ) {
        return new ReactiveRabbitBridgeTopologyAutoConfigurer(rabbitAdmin.getIfAvailable(() -> null), properties);
    }

    @Bean
    @ConditionalOnBean({ConnectionFactory.class, MessageSerializer.class})
    @ConditionalOnMissingBean
    ReactiveRabbitBridgeListenerRegistrar reactiveRabbitBridgeListenerRegistrar(
            ConnectionFactory connectionFactory,
            MessageSerializer serializer,
            RabbitWebFluxBridgeProperties properties,
            ReactiveRabbitBridgeTopologyAutoConfigurer topologyAutoConfigurer,
            ObjectProvider<ReactiveIdempotencyStore> idempotencyStore,
            ObjectProvider<ReactiveRabbitBridgeFailureHandler> failureHandler
    ) {
        return new ReactiveRabbitBridgeListenerRegistrar(
                connectionFactory,
                serializer,
                properties,
                topologyAutoConfigurer,
                idempotencyStore.getIfAvailable(),
                failureHandler.getIfAvailable()
        );
    }

    @Bean
    @ConditionalOnBean({RabbitTemplate.class, MessageSerializer.class})
    @ConditionalOnMissingBean(ReactiveReliablePublisher.class)
    ReactiveRabbitBridgePublisher reactiveRabbitBridgePublisher(
            RabbitTemplate rabbitTemplate,
            MessageSerializer serializer,
            RabbitWebFluxBridgeProperties properties,
            RabbitBridgeExecutorProvider executorProvider,
            RabbitBridgeConcurrencyGuard concurrencyGuard,
            Clock clock
    ) {
        return new ReactiveRabbitBridgePublisher(
                rabbitTemplate,
                serializer,
                properties,
                executorProvider,
                concurrencyGuard,
                clock
        );
    }
}
