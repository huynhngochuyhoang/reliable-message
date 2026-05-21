package io.github.huynhngochuyhoang.reliablemessage.rpc.webflux;

import io.github.huynhngochuyhoang.reliablemessage.rpc.RpcExceptionClassifier;
import io.github.huynhngochuyhoang.reliablemessage.rpc.RpcMetrics;
import io.github.huynhngochuyhoang.reliablemessage.rpc.RpcRetryPolicy;
import io.github.huynhngochuyhoang.reliablemessage.rpc.RpcTimeoutPolicy;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.web.reactive.function.client.WebClientCustomizer;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.web.reactive.function.client.WebClient;

@AutoConfiguration
@ConditionalOnClass(WebClient.class)
@EnableConfigurationProperties(RpcWebFluxProperties.class)
@ConditionalOnProperty(prefix = "message.reliability.rpc.webflux", name = "enabled", havingValue = "true", matchIfMissing = true)
public class RpcWebFluxAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    RpcExceptionClassifier reactiveRpcExceptionClassifier() {
        return RpcExceptionClassifier.defaults();
    }

    @Bean
    @ConditionalOnMissingBean(name = "reactiveRpcMetrics")
    RpcMetrics reactiveRpcMetrics(ObjectProvider<MeterRegistry> meterRegistryProvider) {
        return new RpcMetrics(meterRegistryProvider.getIfAvailable(SimpleMeterRegistry::new), "rpc_reactive");
    }

    @Bean
    @ConditionalOnMissingBean
    RpcWebClientExchangeFilter rpcWebClientExchangeFilter(@Qualifier("reactiveRpcMetrics") RpcMetrics reactiveRpcMetrics, RpcExceptionClassifier reactiveRpcExceptionClassifier) {
        return new RpcWebClientExchangeFilter(reactiveRpcMetrics, reactiveRpcExceptionClassifier);
    }

    @Bean
    @ConditionalOnMissingBean
    ReactiveRpcOperator reactiveRpcOperator(
            RpcWebFluxProperties properties,
            RpcExceptionClassifier reactiveRpcExceptionClassifier,
            @Qualifier("reactiveRpcMetrics") RpcMetrics reactiveRpcMetrics
    ) {
        return new ReactiveRpcOperator(
                new RpcRetryPolicy(properties.getMaxAttempts(), properties.getBackoff()),
                new RpcTimeoutPolicy(properties.getRequestTimeout()),
                reactiveRpcExceptionClassifier,
                reactiveRpcMetrics
        );
    }

    @Bean
    WebClientCustomizer rpcWebClientCustomizer(RpcWebClientExchangeFilter filter) {
        return builder -> builder.filter(filter);
    }
}
