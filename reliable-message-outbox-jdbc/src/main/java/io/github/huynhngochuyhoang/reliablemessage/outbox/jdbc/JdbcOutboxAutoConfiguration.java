package io.github.huynhngochuyhoang.reliablemessage.outbox.jdbc;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.huynhngochuyhoang.reliablemessage.mvc.OutboxPublisher;
import io.github.huynhngochuyhoang.reliablemessage.mvc.OutboxStore;
import io.github.huynhngochuyhoang.reliablemessage.mvc.ReliablePublisher;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.Clock;

@AutoConfiguration
@ConditionalOnClass(JdbcTemplate.class)
@EnableScheduling
@EnableConfigurationProperties(JdbcOutboxProperties.class)
public class JdbcOutboxAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    Clock reliableMessageOutboxClock() {
        return Clock.systemUTC();
    }

    @Bean
    @ConditionalOnBean(JdbcTemplate.class)
    @ConditionalOnMissingBean(OutboxStore.class)
    JdbcOutboxStore jdbcOutboxStore(
            JdbcTemplate jdbcTemplate,
            ObjectProvider<ObjectMapper> objectMapper,
            JdbcOutboxProperties properties,
            Clock clock
    ) {
        JdbcOutboxStore store = new JdbcOutboxStore(jdbcTemplate, objectMapper.getIfAvailable(ObjectMapper::new), clock);
        if (properties.isInitializeSchema()) {
            store.initializeSchema();
        }
        return store;
    }

    @Bean
    @ConditionalOnBean(OutboxStore.class)
    @ConditionalOnMissingBean(OutboxPublisher.class)
    JdbcOutboxPublisher jdbcOutboxPublisher(OutboxStore outboxStore, Clock clock) {
        return new JdbcOutboxPublisher(outboxStore, clock);
    }

    @Bean
    @ConditionalOnBean({OutboxStore.class, ReliablePublisher.class})
    @ConditionalOnMissingBean
    OutboxFlushScheduler outboxFlushScheduler(
            OutboxStore outboxStore,
            ReliablePublisher reliablePublisher,
            JdbcOutboxProperties properties,
            Clock clock
    ) {
        return new OutboxFlushScheduler(outboxStore, reliablePublisher, properties, clock);
    }
}
