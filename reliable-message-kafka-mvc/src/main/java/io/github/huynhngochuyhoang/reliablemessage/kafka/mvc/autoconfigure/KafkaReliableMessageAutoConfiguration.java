package io.github.huynhngochuyhoang.reliablemessage.kafka.mvc.autoconfigure;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.huynhngochuyhoang.reliablemessage.core.serialization.MessageSerializer;
import io.github.huynhngochuyhoang.reliablemessage.kafka.mvc.*;
import io.github.huynhngochuyhoang.reliablemessage.mvc.IdempotencyStore;
import io.github.huynhngochuyhoang.reliablemessage.mvc.ReliablePublisher;
import io.github.huynhngochuyhoang.reliablemessage.observability.MessageObservability;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.observation.ObservationRegistry;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Clock;

@AutoConfiguration(after = KafkaAutoConfiguration.class)
@ConditionalOnClass({KafkaTemplate.class, ConsumerFactory.class})
@ConditionalOnProperty(prefix = "message.reliability", name = "transport", havingValue = "kafka")
@EnableConfigurationProperties(KafkaReliableMessageProperties.class)
public class KafkaReliableMessageAutoConfiguration {

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
    KafkaTopologyAutoConfigurer kafkaTopologyAutoConfigurer(
            ObjectProvider<KafkaAdmin> kafkaAdmin,
            KafkaReliableMessageProperties properties
    ) {
        return new KafkaTopologyAutoConfigurer(kafkaAdmin.getIfAvailable(() -> null), properties);
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(KafkaTemplate.class)
    KafkaRetryStrategy kafkaRetryStrategy(
            KafkaTemplate<String, byte[]> kafkaTemplate,
            KafkaReliableMessageProperties properties,
            MessageObservability observability
    ) {
        return new KafkaRetryStrategy(kafkaTemplate, properties, observability);
    }

    @Bean
    @ConditionalOnMissingBean(ReliablePublisher.class)
    @ConditionalOnBean(KafkaTemplate.class)
    KafkaReliablePublisher reliablePublisher(
            KafkaTemplate<String, byte[]> kafkaTemplate,
            MessageSerializer serializer,
            KafkaReliableMessageProperties properties,
            Clock clock,
            MessageObservability observability
    ) {
        return new KafkaReliablePublisher(kafkaTemplate, serializer, properties, clock, observability);
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(ConsumerFactory.class)
    KafkaReliableListenerRegistrar kafkaReliableListenerRegistrar(
            ConsumerFactory<String, byte[]> consumerFactory,
            MessageSerializer serializer,
            KafkaReliableMessageProperties properties,
            MessageObservability observability,
            KafkaTopologyAutoConfigurer topologyAutoConfigurer,
            ObjectProvider<KafkaRetryStrategy> retryStrategy,
            ObjectProvider<IdempotencyStore> idempotencyStore
    ) {
        return new KafkaReliableListenerRegistrar(
                consumerFactory,
                serializer,
                properties,
                observability,
                topologyAutoConfigurer,
                retryStrategy.getIfAvailable(),
                idempotencyStore.getIfAvailable()
        );
    }
}
