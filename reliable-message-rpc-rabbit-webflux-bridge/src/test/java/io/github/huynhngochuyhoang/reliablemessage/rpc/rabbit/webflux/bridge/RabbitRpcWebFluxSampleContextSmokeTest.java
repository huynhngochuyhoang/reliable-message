package io.github.huynhngochuyhoang.reliablemessage.rpc.rabbit.webflux.bridge;

import io.github.huynhngochuyhoang.reliablemessage.rpc.rabbit.webflux.bridge.autoconfigure.RabbitRpcWebFluxBridgeAutoConfiguration;
import io.github.huynhngochuyhoang.reliablemessage.webflux.ReactiveReliablePublisher;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.rabbit.AsyncRabbitTemplate;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionListener;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static org.assertj.core.api.Assertions.assertThat;

class RabbitRpcWebFluxSampleContextSmokeTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(RabbitRpcWebFluxBridgeAutoConfiguration.class));

    @Test
    void rabbitRpcWebFluxSampleContextLoadsOnlyWhenAsyncRabbitTemplateExists() {
        contextRunner
                .withUserConfiguration(SampleConfig.class)
                .run(context -> {
                    assertThat(context)
                            .hasSingleBean(RabbitRpcWebFluxBridgeProperties.class)
                            .hasSingleBean(RabbitRpcBridgeExecutorProvider.class)
                            .hasSingleBean(ReactiveRabbitRpcClient.class)
                            .doesNotHaveBean(ReactiveReliablePublisher.class)
                            .doesNotHaveBean("reactiveRabbitBridgePublisher")
                            .doesNotHaveBean("reactiveRabbitBridgeListenerRegistrar");
                    assertThat(context.getEnvironment().getProperty("message.reliability.outbox.schema.payload-storage"))
                            .isNull();
                });
    }

    @Test
    void rabbitRpcWebFluxSampleContextDoesNotCreateClientWithoutAsyncRabbitTemplate() {
        contextRunner.run(context -> assertThat(context)
                .hasSingleBean(RabbitRpcWebFluxBridgeProperties.class)
                .doesNotHaveBean(ReactiveRabbitRpcClient.class)
                .doesNotHaveBean(RabbitRpcBridgeExecutorProvider.class));
    }

    @Configuration(proxyBeanMethods = false)
    static class SampleConfig {

        @Bean
        AsyncRabbitTemplate asyncRabbitTemplate() {
            return new NonStartingAsyncRabbitTemplate();
        }
    }

    private static final class NonStartingAsyncRabbitTemplate extends AsyncRabbitTemplate {
        private NonStartingAsyncRabbitTemplate() {
            super(new StubConnectionFactory(), "", "");
        }

        @Override
        public synchronized void start() {
        }

        @Override
        public boolean isRunning() {
            return false;
        }
    }

    private static final class StubConnectionFactory implements ConnectionFactory {
        @Override
        public Connection createConnection() throws AmqpException {
            throw new UnsupportedOperationException("not used");
        }

        @Override
        public String getHost() {
            return "localhost";
        }

        @Override
        public int getPort() {
            return 5672;
        }

        @Override
        public String getVirtualHost() {
            return "/";
        }

        @Override
        public String getUsername() {
            return "guest";
        }

        @Override
        public void addConnectionListener(ConnectionListener listener) {
        }

        @Override
        public boolean removeConnectionListener(ConnectionListener listener) {
            return false;
        }

        @Override
        public void clearConnectionListeners() {
        }
    }
}
