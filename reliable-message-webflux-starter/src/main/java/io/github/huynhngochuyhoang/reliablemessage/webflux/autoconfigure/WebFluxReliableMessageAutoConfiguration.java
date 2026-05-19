package io.github.huynhngochuyhoang.reliablemessage.webflux.autoconfigure;

import io.github.huynhngochuyhoang.reliablemessage.webflux.ReactiveReliableListenerMethodInvoker;
import io.github.huynhngochuyhoang.reliablemessage.webflux.ReactiveReliableListenerRegistrar;
import io.github.huynhngochuyhoang.reliablemessage.webflux.ReactiveReliableMessageProperties;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.web.reactive.DispatcherHandler;
import reactor.core.publisher.Mono;

@AutoConfiguration
@ConditionalOnClass({Mono.class, DispatcherHandler.class})
@EnableConfigurationProperties(ReactiveReliableMessageProperties.class)
public class WebFluxReliableMessageAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    ReactiveReliableListenerRegistrar reactiveReliableListenerRegistrar() {
        return new ReactiveReliableListenerRegistrar();
    }

    @Bean
    @ConditionalOnMissingBean
    ReactiveReliableListenerMethodInvoker reactiveReliableListenerMethodInvoker() {
        return new ReactiveReliableListenerMethodInvoker();
    }
}
