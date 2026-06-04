package io.github.huynhngochuyhoang.reliablemessage.kafka.webflux;

import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessage;
import io.github.huynhngochuyhoang.reliablemessage.kafka.webflux.autoconfigure.ReactiveKafkaReliableMessageAutoConfiguration;
import io.github.huynhngochuyhoang.reliablemessage.webflux.ReactiveReliableListener;
import io.github.huynhngochuyhoang.reliablemessage.webflux.ReactiveReliablePublisher;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaWebFluxSampleContextSmokeTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(ReactiveKafkaReliableMessageAutoConfiguration.class))
            .withUserConfiguration(SampleConfig.class)
            .withPropertyValues(
                    "message.reliability.transport=kafka",
                    "message.reliability.service-name=orders"
            );

    @Test
    void webFluxKafkaSampleContextLoads() {
        contextRunner.run(context -> {
            assertThat(context)
                    .hasSingleBean(ReactiveKafkaReliableMessageProperties.class)
                    .hasSingleBean(ReactiveReliablePublisher.class)
                    .hasSingleBean(ReactiveKafkaReliableListenerRegistrar.class)
                    .doesNotHaveBean("reactiveRabbitRpcClient")
                    .doesNotHaveBean("reactiveRabbitBridgePublisher");
            assertThat(context.getEnvironment().getProperty("message.reliability.outbox.schema.payload-storage"))
                    .isNull();
        });
    }

    @Configuration(proxyBeanMethods = false)
    static class SampleConfig {

        @Bean
        KafkaSender<String, byte[]> kafkaSender() {
            return org.mockito.Mockito.mock(KafkaSender.class);
        }

        @Bean
        SampleListener sampleListener() {
            return new SampleListener();
        }
    }

    static final class SampleListener {

        @ReactiveReliableListener("order.created")
        Mono<Void> onOrderCreated(ReliableMessage<OrderCreated> message) {
            return Mono.empty();
        }
    }

    record OrderCreated(String orderId) {
    }
}
