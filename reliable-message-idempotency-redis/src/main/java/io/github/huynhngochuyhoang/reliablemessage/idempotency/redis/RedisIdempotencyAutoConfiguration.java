package io.github.huynhngochuyhoang.reliablemessage.idempotency.redis;

import io.github.huynhngochuyhoang.reliablemessage.mvc.IdempotencyStore;
import io.github.huynhngochuyhoang.reliablemessage.observability.MessageObservability;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.core.StringRedisTemplate;

@AutoConfiguration
@ConditionalOnClass(StringRedisTemplate.class)
public class RedisIdempotencyAutoConfiguration {

    @Bean
    @ConditionalOnBean(StringRedisTemplate.class)
    @ConditionalOnMissingBean(IdempotencyStore.class)
    RedisIdempotencyStore redisIdempotencyStore(
            StringRedisTemplate redisTemplate,
            MessageObservability observability
    ) {
        return new RedisIdempotencyStore(redisTemplate, "reliable-message:idempotency:", java.time.Clock.systemUTC(), observability);
    }
}
