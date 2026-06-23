package io.github.huynhngochuyhoang.reliablemessage.kafka.webflux.autoconfigure;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.huynhngochuyhoang.reliablemessage.core.serialization.MessageSerializer;
import io.github.huynhngochuyhoang.reliablemessage.kafka.webflux.*;
import io.github.huynhngochuyhoang.reliablemessage.observability.MessageObservability;
import io.github.huynhngochuyhoang.reliablemessage.webflux.ReactiveIdempotencyStore;
import io.github.huynhngochuyhoang.reliablemessage.webflux.ReactiveReliablePublisher;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;

import java.time.Clock;
import java.util.HashMap;
import java.util.Map;

@AutoConfiguration
@ConditionalOnClass({KafkaSender.class, KafkaReceiver.class})
@ConditionalOnProperty(prefix = "message.reliability", name = "transport", havingValue = "kafka")
@EnableConfigurationProperties(ReactiveKafkaReliableMessageProperties.class)
public class ReactiveKafkaReliableMessageAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    Clock reliableMessageReactiveKafkaClock() {
        return Clock.systemUTC();
    }

    @Bean
    @ConditionalOnMissingBean
    MessageSerializer reliableMessageSerializer(ObjectProvider<ObjectMapper> objectMapper) {
        return new JacksonReliableMessageSerializer(objectMapper.getIfAvailable(ObjectMapper::new));
    }

    @Bean
    @ConditionalOnMissingBean
    SenderOptions<String, byte[]> reliableMessageSenderOptions(ReactiveKafkaReliableMessageProperties properties) {
        Map<String, Object> producerProperties = new HashMap<>(properties.getKafka().getProducerProperties());
        producerProperties.putIfAbsent(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProperties.putIfAbsent(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        return SenderOptions.create(producerProperties);
    }

    @Bean
    @ConditionalOnMissingBean
    ReceiverOptions<String, byte[]> reliableMessageReceiverOptions(ReactiveKafkaReliableMessageProperties properties) {
        Map<String, Object> consumerProperties = new HashMap<>(properties.getKafka().getConsumerProperties());
        consumerProperties.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProperties.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        consumerProperties.putIfAbsent(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return ReceiverOptions.create(consumerProperties);
    }

    @Bean
    @ConditionalOnMissingBean
    KafkaSender<String, byte[]> reliableMessageKafkaSender(SenderOptions<String, byte[]> senderOptions) {
        return KafkaSender.create(senderOptions);
    }

    @Bean
    @ConditionalOnMissingBean
    ReactiveKafkaReceiverFactory reliableMessageKafkaReceiverFactory(ReceiverOptions<String, byte[]> receiverOptions) {
        return new DefaultReactiveKafkaReceiverFactory(receiverOptions);
    }

    @Bean
    @ConditionalOnBean(KafkaSender.class)
    @ConditionalOnMissingBean(ReactiveReliablePublisher.class)
    ReactiveKafkaReliablePublisher reactiveReliablePublisher(
            KafkaSender<String, byte[]> kafkaSender,
            MessageSerializer serializer,
            ReactiveKafkaReliableMessageProperties properties,
            Clock clock,
            ObjectProvider<MessageObservability> observability
    ) {
        return new ReactiveKafkaReliablePublisher(
                kafkaSender, serializer, properties, clock, observability.getIfAvailable()
        );
    }

    @Bean
    @ConditionalOnBean(KafkaSender.class)
    @ConditionalOnMissingBean
    ReactiveKafkaRetryStrategy reactiveKafkaRetryStrategy(
            KafkaSender<String, byte[]> kafkaSender,
            ReactiveKafkaReliableMessageProperties properties,
            Clock clock
    ) {
        return new ReactiveKafkaRetryStrategy(kafkaSender, properties, clock);
    }

    @Bean
    @ConditionalOnBean(ReactiveKafkaReceiverFactory.class)
    @ConditionalOnMissingBean
    ReactiveKafkaReliableListenerRegistrar reactiveKafkaReliableListenerRegistrar(
            ReactiveKafkaReceiverFactory receiverFactory,
            MessageSerializer serializer,
            ReactiveKafkaReliableMessageProperties properties,
            ObjectProvider<ReactiveKafkaRetryStrategy> retryStrategy,
            ObjectProvider<ReactiveIdempotencyStore> idempotencyStore,
            ObjectProvider<MessageObservability> observability
    ) {
        return new ReactiveKafkaReliableListenerRegistrar(
                receiverFactory,
                serializer,
                properties,
                retryStrategy.getIfAvailable(),
                idempotencyStore.getIfAvailable(),
                observability.getIfAvailable()
        );
    }
}
