package io.github.huynhngochuyhoang.reliablemessage.outbox.jdbc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.huynhngochuyhoang.reliablemessage.core.MessageStatus;
import io.github.huynhngochuyhoang.reliablemessage.core.PublishOptions;
import io.github.huynhngochuyhoang.reliablemessage.mvc.OutboxMessage;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JdbcOutboxStoreTest {

    private static final Instant NOW = Instant.parse("2026-05-18T00:00:00Z");

    @Test
    void savesOutboxRowInCallerTransaction() {
        TestStore testStore = store();
        testStore.jdbcTemplate.execute("create table orders (id varchar(64) primary key)");
        TransactionTemplate transactionTemplate = new TransactionTemplate(
                new DataSourceTransactionManager(testStore.jdbcTemplate.getDataSource())
        );

        transactionTemplate.executeWithoutResult(status -> {
            testStore.jdbcTemplate.update("insert into orders (id) values (?)", "order-1");
            testStore.store.save(message("event-1"));
        });

        assertEquals(1, testStore.jdbcTemplate.queryForObject("select count(*) from orders", Integer.class));
        assertEquals(1, testStore.jdbcTemplate.queryForObject("select count(*) from message_outbox", Integer.class));
    }

    @Test
    void findsPendingRowsInCreatedOrder() {
        TestStore testStore = store();
        testStore.store.save(message("event-2"));
        testStore.store.save(message("event-1"));

        var messages = testStore.store.findPending(10);

        assertEquals(2, messages.size());
        assertEquals("order.created", messages.getFirst().eventName());
        assertEquals("order-1", ((JsonNode) messages.getFirst().payload()).get("orderId").asText());
    }

    @Test
    void marksPublishedRows() {
        TestStore testStore = store();
        testStore.store.save(message("event-1"));

        testStore.store.markPublished("event-1");

        assertEquals(MessageStatus.PUBLISHED.name(), status(testStore, "event-1"));
        assertEquals(0, testStore.store.findPending(10).size());
    }

    @Test
    void failedRowsAreRetriedAfterNextRetryAt() {
        TestStore testStore = store();
        testStore.store.save(message("event-1"));

        testStore.store.markFailed("event-1", new IllegalStateException("broker down"), NOW.plusSeconds(30));

        assertEquals(0, testStore.store.findPending(10).size());

        testStore.clock = Clock.fixed(NOW.plusSeconds(31), ZoneOffset.UTC);
        JdbcOutboxStore retryStore = new JdbcOutboxStore(testStore.jdbcTemplate, new ObjectMapper(), testStore.clock);

        var messages = retryStore.findPending(10);
        assertEquals(1, messages.size());
        assertEquals(MessageStatus.FAILED, messages.getFirst().status());
        assertEquals(1, messages.getFirst().retryCount());
        assertEquals("broker down", messages.getFirst().lastError());
    }

    private static OutboxMessage message(String id) {
        return new OutboxMessage(
                id,
                "order.created",
                "order-1",
                "event-1",
                "order-1",
                new OrderCreated("order-1"),
                PublishOptions.builder().correlationId("correlation-1").build().headers(),
                MessageStatus.PENDING,
                0,
                null,
                NOW,
                null,
                null
        );
    }

    private static String status(TestStore testStore, String id) {
        return testStore.jdbcTemplate.queryForObject(
                "select status from message_outbox where id = ?",
                String.class,
                id
        );
    }

    private static TestStore store() {
        DriverManagerDataSource dataSource = new DriverManagerDataSource(
                "jdbc:h2:mem:" + UUID.randomUUID() + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1"
        );
        dataSource.setDriverClassName("org.h2.Driver");
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        Clock clock = Clock.fixed(NOW, ZoneOffset.UTC);
        JdbcOutboxStore store = new JdbcOutboxStore(jdbcTemplate, new ObjectMapper(), clock);
        store.initializeSchema();
        return new TestStore(jdbcTemplate, store, clock);
    }

    private record OrderCreated(String orderId) {
    }

    private static class TestStore {
        private final JdbcTemplate jdbcTemplate;
        private final JdbcOutboxStore store;
        private Clock clock;

        private TestStore(JdbcTemplate jdbcTemplate, JdbcOutboxStore store, Clock clock) {
            this.jdbcTemplate = jdbcTemplate;
            this.store = store;
            this.clock = clock;
        }
    }
}
