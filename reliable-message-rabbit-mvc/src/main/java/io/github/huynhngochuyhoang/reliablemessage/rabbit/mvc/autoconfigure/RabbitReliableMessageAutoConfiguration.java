package io.github.huynhngochuyhoang.reliablemessage.rabbit.mvc.autoconfigure;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.huynhngochuyhoang.reliablemessage.core.serialization.MessageSerializer;
import io.github.huynhngochuyhoang.reliablemessage.mvc.IdempotencyStore;
import io.github.huynhngochuyhoang.reliablemessage.mvc.ReliablePublisher;
import io.github.huynhngochuyhoang.reliablemessage.rabbit.mvc.*;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import java.time.Clock;

@AutoConfiguration
@ConditionalOnClass({RabbitTemplate.class, ConnectionFactory.class})
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
    @ConditionalOnMissingBean(ReliablePublisher.class)
    @ConditionalOnBean(RabbitTemplate.class)
    RabbitReliablePublisher reliablePublisher(
            RabbitTemplate rabbitTemplate,
            MessageSerializer serializer,
            RabbitReliableMessageProperties properties,
            Clock clock,
            MeterRegistry meterRegistry
    ) {
        return new RabbitReliablePublisher(rabbitTemplate, serializer, properties, clock, meterRegistry);
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(ConnectionFactory.class)
    RabbitReliableListenerRegistrar rabbitReliableListenerRegistrar(
            ConnectionFactory connectionFactory,
            MessageSerializer serializer,
            RabbitReliableMessageProperties properties,
            MeterRegistry meterRegistry,
            RabbitTopologyAutoConfigurer topologyAutoConfigurer,
            ObjectProvider<IdempotencyStore> idempotencyStore
    ) {
        return new RabbitReliableListenerRegistrar(
                connectionFactory,
                serializer,
                properties,
                meterRegistry,
                topologyAutoConfigurer,
                idempotencyStore.getIfAvailable()
        );
    }
}
