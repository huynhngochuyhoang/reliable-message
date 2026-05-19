package io.github.huynhngochuyhoang.reliablemessage.idempotency.jdbc;

import io.github.huynhngochuyhoang.reliablemessage.mvc.IdempotencyStore;
import io.github.huynhngochuyhoang.reliablemessage.observability.MessageObservability;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;

@AutoConfiguration
@ConditionalOnClass(JdbcTemplate.class)
public class JdbcIdempotencyAutoConfiguration {

    @Bean
    @ConditionalOnBean(JdbcTemplate.class)
    @ConditionalOnMissingBean(IdempotencyStore.class)
    JdbcIdempotencyStore jdbcIdempotencyStore(JdbcTemplate jdbcTemplate, MessageObservability observability) {
        JdbcIdempotencyStore store = new JdbcIdempotencyStore(jdbcTemplate, java.time.Clock.systemUTC(), observability);
        store.initializeSchema();
        return store;
    }
}
