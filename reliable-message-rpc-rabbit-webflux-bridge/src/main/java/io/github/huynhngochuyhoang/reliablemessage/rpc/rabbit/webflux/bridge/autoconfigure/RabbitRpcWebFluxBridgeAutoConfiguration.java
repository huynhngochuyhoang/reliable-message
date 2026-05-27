package io.github.huynhngochuyhoang.reliablemessage.rpc.rabbit.webflux.bridge.autoconfigure;

import io.github.huynhngochuyhoang.reliablemessage.rpc.rabbit.webflux.bridge.RabbitRpcWebFluxBridgeProperties;
import io.github.huynhngochuyhoang.reliablemessage.rpc.rabbit.webflux.bridge.ReactiveRabbitRpcClient;
import io.github.huynhngochuyhoang.reliablemessage.rpc.rabbit.webflux.bridge.UnsupportedReactiveRabbitRpcClient;
import org.springframework.amqp.rabbit.AsyncRabbitTemplate;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

@AutoConfiguration
@ConditionalOnClass(AsyncRabbitTemplate.class)
@ConditionalOnProperty(
        prefix = "message.reliability.rpc.rabbit.webflux",
        name = "enabled",
        havingValue = "true",
        matchIfMissing = true
)
@EnableConfigurationProperties(RabbitRpcWebFluxBridgeProperties.class)
public class RabbitRpcWebFluxBridgeAutoConfiguration {

    @Bean
    @ConditionalOnBean(AsyncRabbitTemplate.class)
    @ConditionalOnMissingBean
    ReactiveRabbitRpcClient reactiveRabbitRpcClient(AsyncRabbitTemplate asyncRabbitTemplate) {
        return new UnsupportedReactiveRabbitRpcClient(asyncRabbitTemplate);
    }
}
