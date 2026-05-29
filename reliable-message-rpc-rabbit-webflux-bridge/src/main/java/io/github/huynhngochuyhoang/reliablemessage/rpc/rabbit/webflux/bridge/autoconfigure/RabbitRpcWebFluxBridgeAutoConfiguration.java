package io.github.huynhngochuyhoang.reliablemessage.rpc.rabbit.webflux.bridge.autoconfigure;

import io.github.huynhngochuyhoang.reliablemessage.rpc.rabbit.webflux.bridge.DefaultReactiveRabbitRpcClient;
import io.github.huynhngochuyhoang.reliablemessage.rpc.rabbit.webflux.bridge.RabbitRpcBridgeExecutorProvider;
import io.github.huynhngochuyhoang.reliablemessage.rpc.rabbit.webflux.bridge.RabbitRpcWebFluxBridgeProperties;
import io.github.huynhngochuyhoang.reliablemessage.rpc.rabbit.webflux.bridge.ReactiveRabbitRpcClient;
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

    @Bean(destroyMethod = "close")
    @ConditionalOnBean(AsyncRabbitTemplate.class)
    @ConditionalOnMissingBean
    RabbitRpcBridgeExecutorProvider rabbitRpcBridgeExecutorProvider(RabbitRpcWebFluxBridgeProperties properties) {
        return RabbitRpcBridgeExecutorProvider.create(properties);
    }

    @Bean
    @ConditionalOnBean(AsyncRabbitTemplate.class)
    @ConditionalOnMissingBean
    ReactiveRabbitRpcClient reactiveRabbitRpcClient(
            AsyncRabbitTemplate asyncRabbitTemplate,
            RabbitRpcWebFluxBridgeProperties properties,
            RabbitRpcBridgeExecutorProvider executorProvider
    ) {
        return new DefaultReactiveRabbitRpcClient(asyncRabbitTemplate, properties, executorProvider);
    }
}
