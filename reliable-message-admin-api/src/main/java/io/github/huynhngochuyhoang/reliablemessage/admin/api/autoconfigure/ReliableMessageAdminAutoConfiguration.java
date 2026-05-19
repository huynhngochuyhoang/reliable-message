package io.github.huynhngochuyhoang.reliablemessage.admin.api.autoconfigure;

import io.github.huynhngochuyhoang.reliablemessage.admin.api.DlqAdminController;
import io.github.huynhngochuyhoang.reliablemessage.admin.api.DlqAdminOperations;
import io.github.huynhngochuyhoang.reliablemessage.admin.api.IdempotencyAdminController;
import io.github.huynhngochuyhoang.reliablemessage.admin.api.IdempotencyAdminOperations;
import io.github.huynhngochuyhoang.reliablemessage.admin.api.OutboxAdminController;
import io.github.huynhngochuyhoang.reliablemessage.admin.api.OutboxAdminOperations;
import io.github.huynhngochuyhoang.reliablemessage.admin.api.ReliableMessageAdminProperties;
import io.github.huynhngochuyhoang.reliablemessage.mvc.OutboxStore;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.web.servlet.DispatcherServlet;

@AutoConfiguration
@ConditionalOnClass(DispatcherServlet.class)
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
@EnableConfigurationProperties(ReliableMessageAdminProperties.class)
@ConditionalOnProperty(prefix = "message.reliability.admin", name = "enabled", havingValue = "true")
public class ReliableMessageAdminAutoConfiguration {

    @Bean
    @ConditionalOnBean(OutboxStore.class)
    @ConditionalOnMissingBean
    OutboxAdminController outboxAdminController(
            OutboxStore outboxStore,
            ReliableMessageAdminProperties properties,
            ObjectProvider<OutboxAdminOperations> operations
    ) {
        return new OutboxAdminController(outboxStore, properties, operations);
    }

    @Bean
    @ConditionalOnBean(DlqAdminOperations.class)
    @ConditionalOnMissingBean
    DlqAdminController dlqAdminController(DlqAdminOperations operations, ReliableMessageAdminProperties properties) {
        return new DlqAdminController(operations, properties);
    }

    @Bean
    @ConditionalOnBean(IdempotencyAdminOperations.class)
    @ConditionalOnMissingBean
    IdempotencyAdminController idempotencyAdminController(IdempotencyAdminOperations operations) {
        return new IdempotencyAdminController(operations);
    }
}
