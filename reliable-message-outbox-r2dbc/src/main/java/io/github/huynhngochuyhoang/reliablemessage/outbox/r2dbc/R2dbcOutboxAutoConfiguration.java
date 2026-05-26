package io.github.huynhngochuyhoang.reliablemessage.outbox.r2dbc;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.huynhngochuyhoang.reliablemessage.webflux.ReactiveOutboxStore;
import io.github.huynhngochuyhoang.reliablemessage.webflux.ReactiveReliablePublisher;
import io.r2dbc.spi.ConnectionFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.time.Clock;

@AutoConfiguration
@AutoConfigureAfter(name = {
        "io.github.huynhngochuyhoang.reliablemessage.kafka.webflux.autoconfigure.ReactiveKafkaReliableMessageAutoConfiguration",
        "io.github.huynhngochuyhoang.reliablemessage.rabbit.webflux.bridge.autoconfigure.RabbitWebFluxBridgeAutoConfiguration"
})
@ConditionalOnClass(DatabaseClient.class)
@EnableConfigurationProperties(R2dbcOutboxProperties.class)
public class R2dbcOutboxAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    Clock reliableMessageR2dbcOutboxClock() {
        return Clock.systemUTC();
    }

    @Bean
    @ConditionalOnBean(ConnectionFactory.class)
    @ConditionalOnMissingBean
    DatabaseClient reliableMessageDatabaseClient(ConnectionFactory connectionFactory) {
        return DatabaseClient.create(connectionFactory);
    }

    @Bean
    @ConditionalOnBean(DatabaseClient.class)
    @ConditionalOnMissingBean(ReactiveOutboxStore.class)
    R2dbcOutboxStore r2dbcOutboxStore(
            DatabaseClient databaseClient,
            ObjectProvider<ObjectMapper> objectMapper,
            Clock clock
    ) {
        return new R2dbcOutboxStore(databaseClient, objectMapper.getIfAvailable(ObjectMapper::new), clock);
    }

    @Configuration(proxyBeanMethods = false)
    @EnableScheduling
    @ConditionalOnProperty(prefix = "message.reliability.outbox", name = "enabled", havingValue = "true")
    @ConditionalOnProperty(prefix = "message.reliability.outbox", name = "flush-enabled", havingValue = "true", matchIfMissing = true)
    static class ReactiveOutboxFlushConfiguration {

        @Bean
        @ConditionalOnBean({ReactiveOutboxStore.class, ReactiveReliablePublisher.class})
        @ConditionalOnMissingBean
        ReactiveOutboxFlushScheduler reactiveOutboxFlushScheduler(
                ReactiveOutboxStore outboxStore,
                ReactiveReliablePublisher reliablePublisher,
                R2dbcOutboxProperties properties,
                Clock clock
        ) {
            return new ReactiveOutboxFlushScheduler(outboxStore, reliablePublisher, properties, clock);
        }
    }
}
