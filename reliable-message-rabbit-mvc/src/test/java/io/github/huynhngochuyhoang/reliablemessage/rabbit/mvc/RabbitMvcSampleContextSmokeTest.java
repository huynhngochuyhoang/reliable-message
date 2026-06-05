package io.github.huynhngochuyhoang.reliablemessage.rabbit.mvc;

import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessage;
import io.github.huynhngochuyhoang.reliablemessage.mvc.ReliableListener;
import io.github.huynhngochuyhoang.reliablemessage.mvc.ReliablePublisher;
import io.github.huynhngochuyhoang.reliablemessage.rabbit.mvc.autoconfigure.RabbitReliableMessageAutoConfiguration;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static org.junit.jupiter.api.Assertions.*;

class RabbitMvcSampleContextSmokeTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(RabbitReliableMessageAutoConfiguration.class))
            .withUserConfiguration(SampleConfig.class)
            .withPropertyValues(
                    "message.reliability.transport=rabbit",
                    "message.reliability.service-name=orders",
                    "message.reliability.rabbit.listener-auto-startup=false"
            );

    @Test
    void mvcRabbitSampleContextLoads() {
        contextRunner.run(context -> {
            assertEquals(1, context.getBeansOfType(RabbitReliableMessageProperties.class).size());
            assertNotNull(context.getBean(ReliablePublisher.class));
            assertNotNull(context.getBean(RabbitReliableListenerRegistrar.class));
            assertFalse(context.containsBean("reactiveRabbitRpcClient"));
            assertFalse(context.containsBean("reactiveRabbitBridgePublisher"));
            assertNull(context.getEnvironment().getProperty("message.reliability.outbox.schema.payload-storage"));
        });
    }

    @Configuration(proxyBeanMethods = false)
    static class SampleConfig {

        @Bean
        ConnectionFactory connectionFactory() {
            return org.mockito.Mockito.mock(ConnectionFactory.class);
        }

        @Bean
        RabbitTemplate rabbitTemplate() {
            return org.mockito.Mockito.mock(RabbitTemplate.class);
        }

        @Bean
        RabbitAdmin rabbitAdmin() {
            return org.mockito.Mockito.mock(RabbitAdmin.class);
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
