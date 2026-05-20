package io.github.huynhngochuyhoang.reliablemessage.audit.webflux;

import io.github.huynhngochuyhoang.reliablemessage.audit.DefaultMessageAuditSanitizer;
import io.github.huynhngochuyhoang.reliablemessage.audit.MessageAuditCapturePolicy;
import io.github.huynhngochuyhoang.reliablemessage.audit.MessageAuditHasher;
import io.github.huynhngochuyhoang.reliablemessage.audit.MessageAuditSanitizer;
import io.github.huynhngochuyhoang.reliablemessage.audit.MessageAuditSigner;
import io.github.huynhngochuyhoang.reliablemessage.audit.NoopMessageAuditSigner;
import io.github.huynhngochuyhoang.reliablemessage.audit.NoopReactiveMessageAuditSink;
import io.github.huynhngochuyhoang.reliablemessage.audit.ReactiveMessageAuditSink;
import io.github.huynhngochuyhoang.reliablemessage.audit.Sha256MessageAuditHasher;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.annotation.Bean;

@AutoConfiguration
@EnableConfigurationProperties(ReactiveMessageAuditProperties.class)
public class ReactiveMessageAuditAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    ReactiveMessageAuditSink reactiveMessageAuditSink() {
        return new NoopReactiveMessageAuditSink();
    }

    @Bean
    @ConditionalOnMissingBean
    MessageAuditCapturePolicy reactiveMessageAuditCapturePolicy(ReactiveMessageAuditProperties properties) {
        return new ReactivePropertiesMessageAuditCapturePolicy(properties);
    }

    @Bean
    @ConditionalOnMissingBean
    MessageAuditSanitizer reactiveMessageAuditSanitizer() {
        return new DefaultMessageAuditSanitizer();
    }

    @Bean
    @ConditionalOnMissingBean
    MessageAuditHasher reactiveMessageAuditHasher() {
        return new Sha256MessageAuditHasher();
    }

    @Bean
    @ConditionalOnMissingBean
    MessageAuditSigner reactiveMessageAuditSigner() {
        return new NoopMessageAuditSigner();
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "message.reliability.audit", name = "enabled", havingValue = "true")
    ReactiveMessageAuditRecorder reactiveMessageAuditRecorder(
            MessageAuditCapturePolicy capturePolicy,
            MessageAuditSanitizer sanitizer,
            MessageAuditHasher hasher,
            MessageAuditSigner signer,
            ReactiveMessageAuditSink sink,
            ObjectProvider<MeterRegistry> meterRegistryProvider,
            ReactiveMessageAuditProperties properties
    ) {
        return new ReactiveMessageAuditRecorder(
                capturePolicy,
                sanitizer,
                hasher,
                signer,
                sink,
                meterRegistryProvider.getIfAvailable(SimpleMeterRegistry::new),
                properties
        );
    }
}
