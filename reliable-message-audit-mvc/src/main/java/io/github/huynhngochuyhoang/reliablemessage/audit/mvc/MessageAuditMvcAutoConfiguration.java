package io.github.huynhngochuyhoang.reliablemessage.audit.mvc;

import io.github.huynhngochuyhoang.reliablemessage.audit.DefaultMessageAuditSanitizer;
import io.github.huynhngochuyhoang.reliablemessage.audit.MessageAuditCapturePolicy;
import io.github.huynhngochuyhoang.reliablemessage.audit.MessageAuditHasher;
import io.github.huynhngochuyhoang.reliablemessage.audit.MessageAuditSanitizer;
import io.github.huynhngochuyhoang.reliablemessage.audit.MessageAuditSigner;
import io.github.huynhngochuyhoang.reliablemessage.audit.MessageAuditSink;
import io.github.huynhngochuyhoang.reliablemessage.audit.NoopMessageAuditSigner;
import io.github.huynhngochuyhoang.reliablemessage.audit.NoopMessageAuditSink;
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
@EnableConfigurationProperties(MessageAuditProperties.class)
public class MessageAuditMvcAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    MessageAuditSink messageAuditSink() {
        return new NoopMessageAuditSink();
    }

    @Bean
    @ConditionalOnMissingBean
    MessageAuditCapturePolicy messageAuditCapturePolicy(MessageAuditProperties properties) {
        return new PropertiesMessageAuditCapturePolicy(properties);
    }

    @Bean
    @ConditionalOnMissingBean
    MessageAuditSanitizer messageAuditSanitizer() {
        return new DefaultMessageAuditSanitizer();
    }

    @Bean
    @ConditionalOnMissingBean
    MessageAuditHasher messageAuditHasher() {
        return new Sha256MessageAuditHasher();
    }

    @Bean
    @ConditionalOnMissingBean
    MessageAuditSigner messageAuditSigner() {
        return new NoopMessageAuditSigner();
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "message.reliability.audit", name = "enabled", havingValue = "true")
    MessageAuditRecorder messageAuditRecorder(
            MessageAuditCapturePolicy capturePolicy,
            MessageAuditSanitizer sanitizer,
            MessageAuditHasher hasher,
            MessageAuditSigner signer,
            MessageAuditSink sink,
            ObjectProvider<MeterRegistry> meterRegistryProvider,
            MessageAuditProperties properties
    ) {
        return new MessageAuditRecorder(
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
