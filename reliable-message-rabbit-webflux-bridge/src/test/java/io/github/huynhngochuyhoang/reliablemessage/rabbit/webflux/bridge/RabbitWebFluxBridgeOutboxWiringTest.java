package io.github.huynhngochuyhoang.reliablemessage.rabbit.webflux.bridge;

import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessage;
import io.github.huynhngochuyhoang.reliablemessage.core.serialization.MessageSerializer;
import io.github.huynhngochuyhoang.reliablemessage.outbox.r2dbc.R2dbcOutboxAutoConfiguration;
import io.github.huynhngochuyhoang.reliablemessage.outbox.r2dbc.ReactiveOutboxFlushScheduler;
import io.github.huynhngochuyhoang.reliablemessage.rabbit.webflux.bridge.autoconfigure.RabbitWebFluxBridgeAutoConfiguration;
import io.github.huynhngochuyhoang.reliablemessage.webflux.OutboxMessage;
import io.github.huynhngochuyhoang.reliablemessage.webflux.ReactiveOutboxStore;
import io.github.huynhngochuyhoang.reliablemessage.webflux.ReactiveReliablePublisher;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Field;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class RabbitWebFluxBridgeOutboxWiringTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(
                    RabbitWebFluxBridgeAutoConfiguration.class,
                    R2dbcOutboxAutoConfiguration.class
            ));

    @Test
    void rabbitWebFluxBridgeCreatesR2dbcOutboxFlusherUsingRabbitBridgePublisher() throws Exception {
        contextRunner
                .withUserConfiguration(OutboxStoreConfig.class)
                .withBean(RabbitTemplate.class, RecordingRabbitTemplate::new)
                .withBean(MessageSerializer.class, RecordingSerializer::new)
                .withPropertyValues(
                        "message.reliability.transport=rabbit",
                        "message.reliability.outbox.enabled=true",
                        "message.reliability.outbox.flush-enabled=true"
                )
                .run(context -> {
                    assertThat(context).hasSingleBean(ReactiveRabbitBridgePublisher.class);
                    assertThat(context).hasSingleBean(ReactiveReliablePublisher.class);
                    assertThat(context).hasSingleBean(ReactiveOutboxFlushScheduler.class);
                    assertThat(flusherPublisher(context.getBean(ReactiveOutboxFlushScheduler.class)))
                            .isSameAs(context.getBean(ReactiveReliablePublisher.class))
                            .isInstanceOf(ReactiveRabbitBridgePublisher.class);
                });
    }

    private static Object flusherPublisher(ReactiveOutboxFlushScheduler scheduler) throws Exception {
        Field field = ReactiveOutboxFlushScheduler.class.getDeclaredField("reliablePublisher");
        field.setAccessible(true);
        return field.get(scheduler);
    }

    @Configuration(proxyBeanMethods = false)
    static class OutboxStoreConfig {

        @Bean
        ReactiveOutboxStore reactiveOutboxStore() {
            return new NoopReactiveOutboxStore();
        }
    }

    private static final class RecordingRabbitTemplate extends RabbitTemplate {
        @Override
        public void afterPropertiesSet() {
        }
    }

    private static final class RecordingSerializer implements MessageSerializer {
        @Override
        public <T> byte[] serialize(ReliableMessage<T> message) {
            return new byte[]{1};
        }

        @Override
        public <T> ReliableMessage<T> deserialize(byte[] content, Class<T> payloadType) {
            throw new UnsupportedOperationException("deserialize is not used by wiring tests");
        }
    }

    private static final class NoopReactiveOutboxStore implements ReactiveOutboxStore {
        @Override
        public Mono<Void> save(OutboxMessage message) {
            return Mono.empty();
        }

        @Override
        public Flux<OutboxMessage> findPending(int limit) {
            return Flux.empty();
        }

        @Override
        public Mono<Void> markPublished(String id) {
            return Mono.empty();
        }

        @Override
        public Mono<Void> markFailed(String id, Throwable error, Instant nextRetryAt) {
            return Mono.empty();
        }
    }
}
