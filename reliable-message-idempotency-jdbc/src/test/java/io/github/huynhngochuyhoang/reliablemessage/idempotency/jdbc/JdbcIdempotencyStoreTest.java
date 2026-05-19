package io.github.huynhngochuyhoang.reliablemessage.idempotency.jdbc;

import io.github.huynhngochuyhoang.reliablemessage.mvc.IdempotencyState;
import io.github.huynhngochuyhoang.reliablemessage.observability.MessageObservability;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.observation.ObservationRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import java.time.Duration;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JdbcIdempotencyStoreTest {

    @Test
    void startsOnceAndTreatsProcessingKeyAsDuplicate() {
        JdbcIdempotencyStore store = store();

        assertTrue(store.tryStart("event-1", Duration.ofMinutes(5)).started());

        var duplicate = store.tryStart("event-1", Duration.ofMinutes(5));
        assertFalse(duplicate.started());
        assertEquals(IdempotencyState.PROCESSING, duplicate.state());
    }

    @Test
    void successfulKeyIsDuplicate() {
        JdbcIdempotencyStore store = store();
        store.tryStart("event-1", Duration.ofMinutes(5));
        store.markSuccess("event-1");

        var duplicate = store.tryStart("event-1", Duration.ofMinutes(5));

        assertFalse(duplicate.started());
        assertEquals(IdempotencyState.SUCCESS, duplicate.state());
    }

    @Test
    void failedKeyCanStartAgain() {
        JdbcIdempotencyStore store = store();
        store.tryStart("event-1", Duration.ofMinutes(5));
        store.markFailed("event-1", new IllegalStateException("boom"));

        assertTrue(store.tryStart("event-1", Duration.ofMinutes(5)).started());
    }

    @Test
    void autoConfiguredStoreInitializesSchema() {
        DriverManagerDataSource dataSource = dataSource();
        JdbcIdempotencyStore store = new JdbcIdempotencyAutoConfiguration().jdbcIdempotencyStore(
                new JdbcTemplate(dataSource),
                new MessageObservability(new SimpleMeterRegistry(), ObservationRegistry.NOOP)
        );

        assertTrue(store.tryStart("event-1", Duration.ofMinutes(5)).started());
    }

    private static JdbcIdempotencyStore store() {
        JdbcIdempotencyStore store = new JdbcIdempotencyStore(new JdbcTemplate(dataSource()));
        store.initializeSchema();
        return store;
    }

    private static DriverManagerDataSource dataSource() {
        DriverManagerDataSource dataSource = new DriverManagerDataSource(
                "jdbc:h2:mem:" + UUID.randomUUID() + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1"
        );
        dataSource.setDriverClassName("org.h2.Driver");
        return dataSource;
    }
}
