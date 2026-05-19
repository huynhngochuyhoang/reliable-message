package io.github.huynhngochuyhoang.reliablemessage.idempotency.redis.reactive;

import io.github.huynhngochuyhoang.reliablemessage.webflux.ReactiveIdempotencyStore;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;

import java.time.Clock;

@AutoConfiguration
@ConditionalOnClass(ReactiveStringRedisTemplate.class)
public class ReactiveRedisIdempotencyAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    Clock reliableMessageReactiveRedisClock() {
        return Clock.systemUTC();
    }

    @Bean
    @ConditionalOnBean(ReactiveStringRedisTemplate.class)
    @ConditionalOnMissingBean(ReactiveIdempotencyStore.class)
    ReactiveRedisIdempotencyStore reactiveRedisIdempotencyStore(
            ReactiveStringRedisTemplate redisTemplate,
            Clock clock
    ) {
        return new ReactiveRedisIdempotencyStore(redisTemplate, "reliable-message:idempotency:", clock);
    }
}
