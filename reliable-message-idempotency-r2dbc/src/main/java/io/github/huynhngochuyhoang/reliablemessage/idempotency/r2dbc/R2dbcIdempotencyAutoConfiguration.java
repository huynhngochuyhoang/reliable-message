package io.github.huynhngochuyhoang.reliablemessage.idempotency.r2dbc;

import io.github.huynhngochuyhoang.reliablemessage.webflux.ReactiveIdempotencyStore;
import io.r2dbc.spi.ConnectionFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.r2dbc.core.DatabaseClient;

import java.time.Clock;

@AutoConfiguration
@ConditionalOnClass(DatabaseClient.class)
public class R2dbcIdempotencyAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    Clock reliableMessageR2dbcIdempotencyClock() {
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
    @ConditionalOnMissingBean(ReactiveIdempotencyStore.class)
    R2dbcIdempotencyStore r2dbcIdempotencyStore(DatabaseClient databaseClient, Clock clock) {
        return new R2dbcIdempotencyStore(databaseClient, clock);
    }
}
