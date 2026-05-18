package io.github.huynhngochuyhoang.reliablemessage.admin.api;

import io.github.huynhngochuyhoang.reliablemessage.admin.api.autoconfigure.ReliableMessageAdminAutoConfiguration;
import io.github.huynhngochuyhoang.reliablemessage.core.MessageStatus;
import io.github.huynhngochuyhoang.reliablemessage.mvc.OutboxMessage;
import io.github.huynhngochuyhoang.reliablemessage.mvc.OutboxStore;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.WebApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReliableMessageAdminAutoConfigurationTest {

    private final WebApplicationContextRunner contextRunner = new WebApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(ReliableMessageAdminAutoConfiguration.class));

    @Test
    void adminControllersAreDisabledByDefault() {
        contextRunner
                .withUserConfiguration(OutboxConfig.class)
                .run(context -> {
                    assertFalse(context.containsBean("outboxAdminController"));
                    assertFalse(context.containsBean("dlqAdminController"));
                    assertFalse(context.containsBean("idempotencyAdminController"));
                });
    }

    @Test
    void createsOutboxControllerOnlyWhenAdminIsEnabled() {
        contextRunner
                .withUserConfiguration(OutboxConfig.class)
                .withPropertyValues("message.reliability.admin.enabled=true")
                .run(context -> {
                    assertTrue(context.containsBean("outboxAdminController"));
                    OutboxAdminController controller = context.getBean(OutboxAdminController.class);

                    List<OutboxMessage> messages = controller.find(500);

                    assertEquals(1, messages.size());
                    assertEquals("outbox-1", messages.getFirst().id());
                    assertEquals(200, context.getBean(TestOutboxStore.class).lastLimit);
                });
    }

    @Test
    void createsDlqAndIdempotencyControllersOnlyWhenOperationsExist() {
        contextRunner
                .withUserConfiguration(OutboxConfig.class, OperationsConfig.class)
                .withPropertyValues("message.reliability.admin.enabled=true")
                .run(context -> {
                    assertTrue(context.containsBean("dlqAdminController"));
                    assertTrue(context.containsBean("idempotencyAdminController"));
                });
    }

    @Configuration(proxyBeanMethods = false)
    static class OutboxConfig {

        @Bean
        TestOutboxStore outboxStore() {
            return new TestOutboxStore();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class OperationsConfig {

        @Bean
        DlqAdminOperations dlqAdminOperations() {
            return new TestDlqAdminOperations();
        }

        @Bean
        IdempotencyAdminOperations idempotencyAdminOperations() {
            return new TestIdempotencyAdminOperations();
        }
    }

    static final class TestOutboxStore implements OutboxStore {
        private int lastLimit;

        @Override
        public void save(OutboxMessage message) {
        }

        @Override
        public List<OutboxMessage> findPending(int limit) {
            this.lastLimit = limit;
            return List.of(new OutboxMessage(
                    "outbox-1",
                    "order.created",
                    null,
                    null,
                    null,
                    Map.of("orderId", "order-1"),
                    Map.of(),
                    MessageStatus.PENDING,
                    0,
                    null,
                    Instant.parse("2026-05-18T00:00:00Z"),
                    null,
                    null
            ));
        }

        @Override
        public void markPublished(String id) {
        }

        @Override
        public void markFailed(String id, Throwable error, Instant nextRetryAt) {
        }
    }

    static final class TestDlqAdminOperations implements DlqAdminOperations {
        private final List<String> retried = new ArrayList<>();

        @Override
        public List<io.github.huynhngochuyhoang.reliablemessage.core.DeadLetterRecord> find(int limit) {
            return List.of();
        }

        @Override
        public void retry(String id) {
            retried.add(id);
        }

        @Override
        public io.github.huynhngochuyhoang.reliablemessage.core.DeadLetterRecord discard(String id, String reason) {
            return null;
        }
    }

    static final class TestIdempotencyAdminOperations implements IdempotencyAdminOperations {

        @Override
        public IdempotencyRecord find(String key) {
            return null;
        }

        @Override
        public void clear(String key) {
        }
    }
}
