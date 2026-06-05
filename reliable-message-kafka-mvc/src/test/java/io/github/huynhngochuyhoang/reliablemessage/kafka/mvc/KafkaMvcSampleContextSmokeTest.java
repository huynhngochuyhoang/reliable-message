package io.github.huynhngochuyhoang.reliablemessage.kafka.mvc;

import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessage;
import io.github.huynhngochuyhoang.reliablemessage.kafka.mvc.autoconfigure.KafkaReliableMessageAutoConfiguration;
import io.github.huynhngochuyhoang.reliablemessage.mvc.ReliableListener;
import io.github.huynhngochuyhoang.reliablemessage.mvc.ReliablePublisher;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;

import static org.junit.jupiter.api.Assertions.*;

class KafkaMvcSampleContextSmokeTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(KafkaReliableMessageAutoConfiguration.class))
            .withUserConfiguration(SampleConfig.class)
            .withPropertyValues(
                    "message.reliability.transport=kafka",
                    "message.reliability.service-name=orders",
                    "message.reliability.kafka.listener-auto-startup=false"
            );

    @Test
    void mvcKafkaSampleContextLoads() {
        contextRunner.run(context -> {
            assertEquals(1, context.getBeansOfType(KafkaReliableMessageProperties.class).size());
            assertNotNull(context.getBean(ReliablePublisher.class));
            assertNotNull(context.getBean(KafkaReliableListenerRegistrar.class));
            assertFalse(context.containsBean("reactiveRabbitRpcClient"));
            assertFalse(context.containsBean("reactiveRabbitBridgePublisher"));
            assertNull(context.getEnvironment().getProperty("message.reliability.outbox.schema.payload-storage"));
        });
    }

    @Configuration(proxyBeanMethods = false)
    static class SampleConfig {

        @Bean
        ConsumerFactory<String, byte[]> consumerFactory() {
            return org.mockito.Mockito.mock(ConsumerFactory.class);
        }

        @Bean
        KafkaTemplate<String, byte[]> kafkaTemplate() {
            return org.mockito.Mockito.mock(KafkaTemplate.class);
        }

        @Bean
        KafkaAdmin kafkaAdmin() {
            return org.mockito.Mockito.mock(KafkaAdmin.class);
        }

        @Bean
        SampleListener sampleListener() {
            return new SampleListener();
        }
    }

    static final class SampleListener {

        @ReliableListener("order.created")
        void onOrderCreated(ReliableMessage<OrderCreated> message) {
        }
    }

    record OrderCreated(String orderId) {
    }
}
