package io.github.huynhngochuyhoang.reliablemessage.outbox.r2dbc;

import io.github.huynhngochuyhoang.reliablemessage.webflux.OutboxMessage;
import io.github.huynhngochuyhoang.reliablemessage.webflux.ReactiveOutboxStore;
import io.github.huynhngochuyhoang.reliablemessage.webflux.ReactiveReliablePublisher;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.ScheduledAnnotationBeanPostProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class R2dbcOutboxAutoConfigurationTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(R2dbcOutboxAutoConfiguration.class));

    @Test
    void doesNotCreateSchedulerWhenOutboxEnabledIsMissing() {
        contextRunner
                .withUserConfiguration(StoreAndPublisherConfig.class)
                .run(context -> assertThat(context)
                        .hasSingleBean(ReactiveOutboxStore.class)
                        .hasSingleBean(ReactiveReliablePublisher.class)
                        .doesNotHaveBean(ReactiveOutboxFlushScheduler.class));
    }

    @Test
    void doesNotCreateSchedulerWhenOutboxIsDisabled() {
        contextRunner
                .withUserConfiguration(StoreAndPublisherConfig.class)
                .withPropertyValues("message.reliability.outbox.enabled=false")
                .run(context -> assertThat(context)
                        .doesNotHaveBean(ReactiveOutboxFlushScheduler.class));
    }

    @Test
    void doesNotCreateSchedulerWhenFlushIsDisabled() {
        contextRunner
                .withUserConfiguration(StoreAndPublisherConfig.class)
                .withPropertyValues(
                        "message.reliability.outbox.enabled=true",
                        "message.reliability.outbox.flush-enabled=false"
                )
                .run(context -> assertThat(context)
                        .doesNotHaveBean(ReactiveOutboxFlushScheduler.class));
    }

    @Test
    void createsSchedulerWhenOutboxAndFlushAreEnabledWithStoreAndPublisher() {
        contextRunner
                .withUserConfiguration(StoreAndPublisherConfig.class)
                .withPropertyValues(
                        "message.reliability.outbox.enabled=true",
                        "message.reliability.outbox.flush-enabled=true"
                )
                .run(context -> assertThat(context)
                        .hasSingleBean(R2dbcOutboxProperties.class)
                        .hasSingleBean(ReactiveOutboxFlushScheduler.class));
    }

    @Test
    void doesNotCreateSchedulerWithoutReactiveReliablePublisher() {
        contextRunner
                .withUserConfiguration(StoreOnlyConfig.class)
                .withPropertyValues(
                        "message.reliability.outbox.enabled=true",
                        "message.reliability.outbox.flush-enabled=true"
                )
                .run(context -> assertThat(context)
                        .hasSingleBean(ReactiveOutboxStore.class)
                        .doesNotHaveBean(ReactiveReliablePublisher.class)
                        .doesNotHaveBean(ReactiveOutboxFlushScheduler.class)
                        .doesNotHaveBean(ScheduledAnnotationBeanPostProcessor.class));
    }

    @Test
    void enablesSchedulingOnlyWhenOutboxFlusherCanBeCreated() {
        contextRunner
                .withUserConfiguration(StoreAndPublisherConfig.class)
                .withPropertyValues(
                        "message.reliability.outbox.enabled=true",
                        "message.reliability.outbox.flush-enabled=true"
                )
                .run(context -> assertThat(context)
                        .hasSingleBean(ReactiveOutboxFlushScheduler.class)
                        .hasSingleBean(ScheduledAnnotationBeanPostProcessor.class));
    }

    @Test
    void createsStoreButNotSchedulerWhenOnlyConnectionFactoryExists() {
        contextRunner
                .withBean(ConnectionFactory.class, () -> ConnectionFactories.get("r2dbc:h2:mem:///outbox-auto-config;MODE=PostgreSQL;DB_CLOSE_DELAY=-1"))
                .withPropertyValues("message.reliability.outbox.enabled=true")
                .run(context -> assertThat(context)
                        .hasSingleBean(ReactiveOutboxStore.class)
                        .doesNotHaveBean(ReactiveOutboxFlushScheduler.class));
    }

    @Test
    void schemaPropertiesBindAndResolveExplicitColumnTypes() {
        contextRunner
                .withPropertyValues(
                        "message.reliability.outbox.schema.payload-storage=json",
                        "message.reliability.outbox.schema.payload-column-type=json",
                        "message.reliability.outbox.schema.headers-column-type=json",
                        "message.reliability.outbox.schema.payload-bytes-column-type=bytea",
                        "message.reliability.outbox.schema.last-error-column-type=clob"
                )
                .run(context -> {
                    OutboxSchema schema = context.getBean(OutboxSchema.class);

                    assertThat(schema.payloadColumnType()).isEqualTo("json");
                    assertThat(schema.headersColumnType()).isEqualTo("json");
                    assertThat(schema.payloadBytesColumnType()).isEqualTo("bytea");
                    assertThat(schema.lastErrorColumnType()).isEqualTo("clob");
                });
    }

    @Test
    void invalidPayloadStoragePropertyFailsStartupClearly() {
        contextRunner
                .withPropertyValues("message.reliability.outbox.schema.payload-storage=invalid")
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure()).hasRootCauseInstanceOf(IllegalArgumentException.class);
                });
    }

    @Test
    void binaryPayloadStoragePropertyFailsStartupClearly() {
        contextRunner
                .withPropertyValues("message.reliability.outbox.schema.payload-storage=binary")
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure())
                            .hasRootCauseInstanceOf(IllegalArgumentException.class)
                            .rootCause()
                            .hasMessage("binary payload storage requires runtime codec/storage support that is not implemented yet");
                });
    }


    @Configuration(proxyBeanMethods = false)
    static class StoreAndPublisherConfig {

        @Bean
        ReactiveOutboxStore reactiveOutboxStore() {
            return new NoopReactiveOutboxStore();
        }

        @Bean
        ReactiveReliablePublisher reactiveReliablePublisher() {
            return (eventName, payload, options) -> Mono.empty();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class StoreOnlyConfig {

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
