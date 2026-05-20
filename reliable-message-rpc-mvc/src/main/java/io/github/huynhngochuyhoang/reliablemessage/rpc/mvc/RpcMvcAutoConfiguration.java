package io.github.huynhngochuyhoang.reliablemessage.rpc.mvc;

import io.github.huynhngochuyhoang.reliablemessage.rpc.RpcExceptionClassifier;
import io.github.huynhngochuyhoang.reliablemessage.rpc.RpcMetrics;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.web.client.RestClientCustomizer;
import org.springframework.context.annotation.Bean;
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
    @ConditionalOnBean(MeterRegistry.class)
    @ConditionalOnMissingBean
    RpcMetrics rpcMvcMetrics(MeterRegistry meterRegistry) {
        return new RpcMetrics(meterRegistry, "rpc_client");
    }

    @Bean
    @ConditionalOnMissingBean
    RpcRestClientInterceptor rpcRestClientInterceptor(RpcMetrics rpcMvcMetrics, RpcExceptionClassifier rpcExceptionClassifier) {
        return new RpcRestClientInterceptor(rpcMvcMetrics, rpcExceptionClassifier);
    }

    @Bean
    RestClientCustomizer rpcRestClientCustomizer(RpcRestClientInterceptor interceptor) {
        return builder -> builder.requestInterceptor(interceptor);
    }
}
