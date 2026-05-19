package io.github.huynhngochuyhoang.reliablemessage.kafka.mvc;

import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessage;
import io.github.huynhngochuyhoang.reliablemessage.kafka.mvc.autoconfigure.KafkaReliableMessageAutoConfiguration;
import io.github.huynhngochuyhoang.reliablemessage.mvc.ReliableListener;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;

class KafkaReliableListenerRegistrarTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withUserConfiguration(TestConfig.class)
            .withConfiguration(org.springframework.boot.autoconfigure.AutoConfigurations.of(
                    KafkaReliableMessageAutoConfiguration.class
            ))
            .withPropertyValues(
                    "message.reliability.transport=kafka",
                    "message.reliability.service-name=order-service",
                    "message.reliability.kafka.topic-prefix=app.",
                    "message.reliability.kafka.listener-auto-startup=false"
            );

    @Test
    void discoversReliableListenersAndCreatesManualAckContainers() {
        contextRunner.run(context -> {
            KafkaReliableListenerRegistrar registrar = context.getBean(KafkaReliableListenerRegistrar.class);

            assertEquals(1, registrar.containers().size());
            assertFalse(registrar.containers().getFirst().isRunning());
            assertEquals(Set.of(
                    "app.order.created",
                    "app.order.created.order-service.retry.5s",
                    "app.order.created.order-service.retry.30s",
                    "app.order.created.order-service.retry.1m",
                    "app.order.created.order-service.retry.5m"
            ), Set.of(registrar.containers().getFirst().getContainerProperties().getTopics()));
            assertEquals("order-service", registrar.containers().getFirst().getContainerProperties().getGroupId());

            KafkaAdmin kafkaAdmin = context.getBean(KafkaAdmin.class);
            org.mockito.ArgumentCaptor<NewTopic[]> topicsCaptor = org.mockito.ArgumentCaptor.forClass(NewTopic[].class);
            verify(kafkaAdmin).createOrModifyTopics(topicsCaptor.capture());
            Set<String> topicNames = Arrays.stream(topicsCaptor.getValue())
                    .map(NewTopic::name)
                    .collect(Collectors.toSet());
            assertEquals(Set.of(
                    "app.order.created",
                    "app.order.created.order-service.dlt",
                    "app.order.created.order-service.retry.5s",
                    "app.order.created.order-service.retry.30s",
                    "app.order.created.order-service.retry.1m",
                    "app.order.created.order-service.retry.5m"
            ), topicNames);
        });
    }

    @Configuration(proxyBeanMethods = false)
    static class TestConfig {

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
