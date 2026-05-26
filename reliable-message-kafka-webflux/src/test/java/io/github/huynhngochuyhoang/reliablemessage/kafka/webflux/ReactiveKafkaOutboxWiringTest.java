package io.github.huynhngochuyhoang.reliablemessage.kafka.webflux;

import io.github.huynhngochuyhoang.reliablemessage.kafka.webflux.autoconfigure.ReactiveKafkaReliableMessageAutoConfiguration;
import io.github.huynhngochuyhoang.reliablemessage.outbox.r2dbc.R2dbcOutboxAutoConfiguration;
import io.github.huynhngochuyhoang.reliablemessage.outbox.r2dbc.ReactiveOutboxFlushScheduler;
import io.github.huynhngochuyhoang.reliablemessage.webflux.OutboxMessage;
import io.github.huynhngochuyhoang.reliablemessage.webflux.ReactiveOutboxStore;
import io.github.huynhngochuyhoang.reliablemessage.webflux.ReactiveReliablePublisher;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;

import java.lang.reflect.Field;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class ReactiveKafkaOutboxWiringTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(
                    ReactiveKafkaReliableMessageAutoConfiguration.class,
                    R2dbcOutboxAutoConfiguration.class
            ));

    @Test
    void kafkaWebFluxCreatesR2dbcOutboxFlusherUsingKafkaReactivePublisher() throws Exception {
        contextRunner
                .withUserConfiguration(OutboxStoreConfig.class)
                .withBean(KafkaSender.class, () -> org.mockito.Mockito.mock(KafkaSender.class))
                .withPropertyValues(
                        "message.reliability.transport=kafka",
                        "message.reliability.outbox.enabled=true",
                        "message.reliability.outbox.flush-enabled=true"
                )
                .run(context -> {
                    assertThat(context).hasSingleBean(ReactiveKafkaReliablePublisher.class);
                    assertThat(context).hasSingleBean(ReactiveReliablePublisher.class);
                    assertThat(context).hasSingleBean(ReactiveOutboxFlushScheduler.class);
                    assertThat(flusherPublisher(context.getBean(ReactiveOutboxFlushScheduler.class)))
                            .isSameAs(context.getBean(ReactiveReliablePublisher.class))
                            .isInstanceOf(ReactiveKafkaReliablePublisher.class);
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
