package io.github.huynhngochuyhoang.reliablemessage.rabbit.mvc.autoconfigure;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.huynhngochuyhoang.reliablemessage.core.serialization.MessageSerializer;
import io.github.huynhngochuyhoang.reliablemessage.mvc.IdempotencyStore;
import io.github.huynhngochuyhoang.reliablemessage.mvc.ReliablePublisher;
import io.github.huynhngochuyhoang.reliablemessage.observability.MessageObservability;
import io.github.huynhngochuyhoang.reliablemessage.rabbit.mvc.*;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.observation.ObservationRegistry;
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
@ConditionalOnClass({RabbitTemplate.class, ConnectionFactory.class})
@ConditionalOnProperty(prefix = "message.reliability", name = "transport", havingValue = "rabbit", matchIfMissing = true)
@EnableConfigurationProperties(RabbitReliableMessageProperties.class)
public class RabbitReliableMessageAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    Clock reliableMessageClock() {
        return Clock.systemUTC();
    }

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

    @Bean
    @ConditionalOnMissingBean
    MessageSerializer reliableMessageSerializer(ObjectProvider<ObjectMapper> objectMapper) {
        return new JacksonReliableMessageSerializer(objectMapper.getIfAvailable(ObjectMapper::new));
    }

    @Bean
    @ConditionalOnMissingBean
    RabbitTopologyAutoConfigurer rabbitTopologyAutoConfigurer(
            ObjectProvider<RabbitAdmin> rabbitAdmin,
            RabbitReliableMessageProperties properties
    ) {
        return new RabbitTopologyAutoConfigurer(rabbitAdmin.getIfAvailable(() -> null), properties);
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(RabbitTemplate.class)
    RabbitRetryStrategy rabbitRetryStrategy(
            RabbitTemplate rabbitTemplate,
            RabbitReliableMessageProperties properties,
            MessageObservability observability
    ) {
        return new RabbitRetryStrategy(rabbitTemplate, properties, observability);
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(RabbitTemplate.class)
    RabbitDlqService rabbitDlqService(
            RabbitTemplate rabbitTemplate,
            RabbitReliableMessageProperties properties,
            Clock clock,
            MessageObservability observability
    ) {
        return new RabbitDlqService(rabbitTemplate, properties, clock, observability);
    }

    @Bean
    @ConditionalOnMissingBean(ReliablePublisher.class)
    @ConditionalOnBean(RabbitTemplate.class)
    RabbitReliablePublisher reliablePublisher(
            RabbitTemplate rabbitTemplate,
            MessageSerializer serializer,
            RabbitReliableMessageProperties properties,
            Clock clock,
            MessageObservability observability
    ) {
        return new RabbitReliablePublisher(rabbitTemplate, serializer, properties, clock, observability);
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(ConnectionFactory.class)
    RabbitReliableListenerRegistrar rabbitReliableListenerRegistrar(
            ConnectionFactory connectionFactory,
            MessageSerializer serializer,
            RabbitReliableMessageProperties properties,
            MessageObservability observability,
            RabbitTopologyAutoConfigurer topologyAutoConfigurer,
            ObjectProvider<RabbitRetryStrategy> retryStrategy,
            ObjectProvider<IdempotencyStore> idempotencyStore
    ) {
        return new RabbitReliableListenerRegistrar(
                connectionFactory,
                serializer,
                properties,
                observability,
                topologyAutoConfigurer,
                retryStrategy.getIfAvailable(),
                idempotencyStore.getIfAvailable()
        );
    }
}
