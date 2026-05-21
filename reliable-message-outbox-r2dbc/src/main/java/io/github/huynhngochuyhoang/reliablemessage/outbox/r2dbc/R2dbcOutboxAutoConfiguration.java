package io.github.huynhngochuyhoang.reliablemessage.outbox.r2dbc;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.huynhngochuyhoang.reliablemessage.webflux.ReactiveOutboxStore;
import io.r2dbc.spi.ConnectionFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.r2dbc.core.DatabaseClient;

import java.time.Clock;

@AutoConfiguration
@ConditionalOnClass(DatabaseClient.class)
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
}
