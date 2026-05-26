package io.github.huynhngochuyhoang.reliablemessage.rpc.webflux;

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

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class RpcWebFluxOutboxWiringTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(
                    RpcWebFluxAutoConfiguration.class,
                    R2dbcOutboxAutoConfiguration.class
            ));

    @Test
    void rpcWebFluxDoesNotCreateR2dbcOutboxFlusher() {
        contextRunner
                .withUserConfiguration(OutboxStoreConfig.class)
                .withPropertyValues(
                        "message.reliability.outbox.enabled=true",
                        "message.reliability.outbox.flush-enabled=true"
                )
                .run(context -> assertThat(context)
                        .doesNotHaveBean(ReactiveReliablePublisher.class)
                        .doesNotHaveBean(ReactiveOutboxFlushScheduler.class));
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
