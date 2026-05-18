package io.github.huynhngochuyhoang.reliablemessage.rabbit.mvc;

import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessage;
import io.github.huynhngochuyhoang.reliablemessage.mvc.ReliableListener;
import io.github.huynhngochuyhoang.reliablemessage.rabbit.mvc.autoconfigure.RabbitReliableMessageAutoConfiguration;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentCaptor.forClass;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class RabbitReliableListenerRegistrarTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withUserConfiguration(TestConfig.class)
            .withConfiguration(org.springframework.boot.autoconfigure.AutoConfigurations.of(
                    RabbitReliableMessageAutoConfiguration.class
            ))
            .withPropertyValues(
                    "message.reliability.service-name=order-service",
                    "message.reliability.rabbit.listener-auto-startup=false"
            );

    @Test
    void discoversReliableListenersAndCreatesManualAckContainers() {
        contextRunner.run(context -> {
            RabbitReliableListenerRegistrar registrar = context.getBean(RabbitReliableListenerRegistrar.class);

            assertEquals(1, registrar.containers().size());
            assertEquals("order-service.order.created", registrar.containers().getFirst().getQueueNames()[0]);
            assertFalse(registrar.containers().getFirst().isRunning());

            RabbitAdmin rabbitAdmin = context.getBean(RabbitAdmin.class);
            verify(rabbitAdmin).declareExchange(any(DirectExchange.class));
            verify(rabbitAdmin, times(6)).declareBinding(any(Binding.class));

            org.mockito.ArgumentCaptor<Queue> queueCaptor = forClass(Queue.class);
            verify(rabbitAdmin, times(6)).declareQueue(queueCaptor.capture());
            Set<String> queueNames = queueCaptor.getAllValues().stream()
                    .map(Queue::getName)
                    .collect(Collectors.toSet());
            assertEquals(Set.of(
                    "order-service.order.created",
                    "order-service.order.created.dlq",
                    "order-service.order.created.retry.5s",
                    "order-service.order.created.retry.30s",
                    "order-service.order.created.retry.1m",
                    "order-service.order.created.retry.5m"
            ), queueNames);
        });
    }

    @Configuration(proxyBeanMethods = false)
    static class TestConfig {

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
        TestListener testListener() {
            return new TestListener();
        }
    }

    static final class TestListener {

        @ReliableListener("order.created")
        void handle(ReliableMessage<OrderCreated> message) {
        }
    }

    record OrderCreated(String orderId) {
    }
}
