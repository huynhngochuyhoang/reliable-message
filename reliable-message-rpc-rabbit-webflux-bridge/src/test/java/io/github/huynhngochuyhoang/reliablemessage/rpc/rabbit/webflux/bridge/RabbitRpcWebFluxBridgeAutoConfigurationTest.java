package io.github.huynhngochuyhoang.reliablemessage.rpc.rabbit.webflux.bridge;

import io.github.huynhngochuyhoang.reliablemessage.outbox.r2dbc.R2dbcOutboxAutoConfiguration;
import io.github.huynhngochuyhoang.reliablemessage.outbox.r2dbc.ReactiveOutboxFlushScheduler;
import io.github.huynhngochuyhoang.reliablemessage.rpc.rabbit.webflux.bridge.autoconfigure.RabbitRpcWebFluxBridgeAutoConfiguration;
import io.github.huynhngochuyhoang.reliablemessage.webflux.OutboxMessage;
import io.github.huynhngochuyhoang.reliablemessage.webflux.ReactiveOutboxStore;
import io.github.huynhngochuyhoang.reliablemessage.webflux.ReactiveReliablePublisher;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.rabbit.AsyncRabbitTemplate;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class RabbitRpcWebFluxBridgeAutoConfigurationTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(RabbitRpcWebFluxBridgeAutoConfiguration.class));

    @Test
    void createsReactiveRabbitRpcClientOnlyWhenAsyncRabbitTemplateExists() {
        contextRunner
                .withBean(AsyncRabbitTemplate.class, RabbitRpcWebFluxBridgeAutoConfigurationTest::asyncRabbitTemplate)
                .run(context -> assertThat(context)
                        .hasSingleBean(RabbitRpcWebFluxBridgeProperties.class)
                        .hasSingleBean(ReactiveRabbitRpcClient.class));
    }

    @Test
    void doesNotCreateReactiveRabbitRpcClientWithoutAsyncRabbitTemplate() {
        contextRunner.run(context -> assertThat(context)
                .hasSingleBean(RabbitRpcWebFluxBridgeProperties.class)
                .doesNotHaveBean(ReactiveRabbitRpcClient.class));
    }

    @Test
    void backsOffWhenDisabled() {
        contextRunner
                .withPropertyValues("message.reliability.rpc.rabbit.webflux.enabled=false")
                .withBean(AsyncRabbitTemplate.class, RabbitRpcWebFluxBridgeAutoConfigurationTest::asyncRabbitTemplate)
                .run(context -> assertThat(context)
                        .doesNotHaveBean(RabbitRpcWebFluxBridgeProperties.class)
                        .doesNotHaveBean(ReactiveRabbitRpcClient.class));
    }

    @Test
    void doesNotCreateEventMessagingBeans() {
        contextRunner
                .withBean(AsyncRabbitTemplate.class, RabbitRpcWebFluxBridgeAutoConfigurationTest::asyncRabbitTemplate)
                .run(context -> assertThat(context)
                        .hasSingleBean(ReactiveRabbitRpcClient.class)
                        .doesNotHaveBean(ReactiveReliablePublisher.class)
                        .doesNotHaveBean("reactiveRabbitBridgeListenerRegistrar"));
    }

    @Test
    void doesNotCreateOutboxFlusherEvenWhenOutboxStoreExists() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(
                        RabbitRpcWebFluxBridgeAutoConfiguration.class,
                        R2dbcOutboxAutoConfiguration.class
                ))
                .withUserConfiguration(OutboxStoreConfig.class)
                .withBean(AsyncRabbitTemplate.class, RabbitRpcWebFluxBridgeAutoConfigurationTest::asyncRabbitTemplate)
                .withPropertyValues(
                        "message.reliability.outbox.enabled=true",
                        "message.reliability.outbox.flush-enabled=true"
                )
                .run(context -> assertThat(context)
                        .hasSingleBean(ReactiveRabbitRpcClient.class)
                        .doesNotHaveBean(ReactiveReliablePublisher.class)
                        .doesNotHaveBean(ReactiveOutboxFlushScheduler.class));
    }

    @Test
    void reactiveRabbitRpcClientDoesNotImplementReactiveReliablePublisher() {
        assertThat(ReactiveReliablePublisher.class.isAssignableFrom(ReactiveRabbitRpcClient.class)).isFalse();
    }

    @Test
    void mainSourcesDoNotUseRabbitTemplateOutboxOrEventBridgeClasses() throws IOException {
        String source = mainSources();

        assertThat(source)
                .doesNotContain("import org.springframework.amqp.rabbit.core.RabbitTemplate")
                .doesNotContain("new RabbitTemplate")
                .contains("AsyncRabbitTemplate")
                .doesNotContain("ReactiveOutboxStore")
                .doesNotContain("ReactiveOutboxFlushScheduler")
                .doesNotContain("ReactiveRabbitBridgePublisher")
                .doesNotContain("ReactiveRabbitBridgeListenerRegistrar")
                .doesNotContain("ReactiveReliablePublisher");
    }

    @Test
    void packageAndClassNamesAreRpcSpecific() {
        assertThat(ReactiveRabbitRpcClient.class.getName()).contains(".rpc.rabbit.webflux.bridge.");
        assertThat(RabbitRpcWebFluxBridgeAutoConfiguration.class.getSimpleName()).contains("Rpc");
        assertThat(RabbitRpcWebFluxBridgeProperties.class.getSimpleName()).contains("Rpc");
    }

    private static AsyncRabbitTemplate asyncRabbitTemplate() {
        return new RecordingAsyncRabbitTemplate();
    }

    private static final class RecordingAsyncRabbitTemplate extends AsyncRabbitTemplate {
        private RecordingAsyncRabbitTemplate() {
            super(new RabbitTemplate());
        }

        @Override
        public synchronized void start() {
        }

        @Override
        public boolean isRunning() {
            return false;
        }
    }

    private static String mainSources() throws IOException {
        Path root = Path.of("src/main/java");
        StringBuilder source = new StringBuilder();
        try (var paths = Files.walk(root)) {
            for (Path path : paths.filter(Files::isRegularFile).toList()) {
                source.append(Files.readString(path)).append('\n');
            }
        }
        return source.toString();
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
