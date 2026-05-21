package io.github.huynhngochuyhoang.reliablemessage.rabbit.webflux.bridge.autoconfigure;

import io.github.huynhngochuyhoang.reliablemessage.rabbit.webflux.bridge.RabbitWebFluxBridgeProperties;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@AutoConfiguration
@ConditionalOnProperty(prefix = "message.reliability.rabbit.bridge", name = "enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(RabbitWebFluxBridgeProperties.class)
public class RabbitWebFluxBridgeAutoConfiguration {
}
