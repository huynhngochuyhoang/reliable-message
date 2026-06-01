package io.github.huynhngochuyhoang.reliablemessage.rpc.rabbit.webflux.bridge.autoconfigure;

import io.github.huynhngochuyhoang.reliablemessage.rpc.rabbit.webflux.bridge.*;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.amqp.rabbit.AsyncRabbitTemplate;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import java.util.List;

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
    RabbitRpcMetrics rabbitRpcMetrics(
            RabbitRpcWebFluxBridgeProperties properties,
            ObjectProvider<MeterRegistry> meterRegistryProvider
    ) {
        List<MeterRegistry> registries = meterRegistryProvider.stream().toList();
        if (registries.isEmpty()) {
            return RabbitRpcMetrics.noop(properties.getExecutorMode());
        }
        MeterRegistry meterRegistry = meterRegistryProvider.getIfUnique();
        if (meterRegistry == null) {
            throw new IllegalStateException("Multiple MeterRegistry beans found; mark one primary for Rabbit RPC bridge metrics");
        }
        return new RabbitRpcMetrics(meterRegistry, properties.getExecutorMode());
    }

    @Bean
    @ConditionalOnBean(AsyncRabbitTemplate.class)
    @ConditionalOnMissingBean
    ReactiveRabbitRpcClient reactiveRabbitRpcClient(
            AsyncRabbitTemplate asyncRabbitTemplate,
            RabbitRpcWebFluxBridgeProperties properties,
            RabbitRpcBridgeExecutorProvider executorProvider,
            RabbitRpcMetrics metrics
    ) {
        return new DefaultReactiveRabbitRpcClient(asyncRabbitTemplate, properties, executorProvider, metrics);
    }
}
