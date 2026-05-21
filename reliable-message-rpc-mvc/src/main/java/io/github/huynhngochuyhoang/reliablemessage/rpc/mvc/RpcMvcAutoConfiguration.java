package io.github.huynhngochuyhoang.reliablemessage.rpc.mvc;

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
import org.springframework.boot.web.client.RestClientCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.web.client.RestClient;

@AutoConfiguration
@ConditionalOnClass(RestClient.class)
@EnableConfigurationProperties(RpcMvcProperties.class)
@ConditionalOnProperty(prefix = "message.reliability.rpc.mvc", name = "enabled", havingValue = "true", matchIfMissing = true)
public class RpcMvcAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    RpcExceptionClassifier rpcExceptionClassifier() {
        return RpcExceptionClassifier.defaults();
    }

    @Bean
    @ConditionalOnMissingBean
    RpcMetrics rpcMvcMetrics(ObjectProvider<MeterRegistry> meterRegistryProvider) {
        return new RpcMetrics(meterRegistryProvider.getIfAvailable(SimpleMeterRegistry::new), "rpc_client");
    }

    @Bean
    @ConditionalOnMissingBean
    RpcRetryPolicy rpcMvcRetryPolicy(RpcMvcProperties properties) {
        return new RpcRetryPolicy(properties.getMaxAttempts(), properties.getBackoff());
    }

    @Bean
    @ConditionalOnMissingBean
    RpcTimeoutPolicy rpcMvcTimeoutPolicy(RpcMvcProperties properties) {
        return new RpcTimeoutPolicy(properties.getRequestTimeout());
    }

    @Bean
    @ConditionalOnMissingBean
    RpcRestClientInterceptor rpcRestClientInterceptor(
            RpcMetrics rpcMvcMetrics,
            RpcExceptionClassifier rpcExceptionClassifier,
            RpcRetryPolicy rpcMvcRetryPolicy,
            RpcTimeoutPolicy rpcMvcTimeoutPolicy
    ) {
        return new RpcRestClientInterceptor(rpcMvcMetrics, rpcExceptionClassifier, rpcMvcRetryPolicy, rpcMvcTimeoutPolicy);
    }

    @Bean
    RestClientCustomizer rpcRestClientCustomizer(RpcRestClientInterceptor interceptor) {
        return builder -> builder.requestInterceptor(interceptor);
    }
}
