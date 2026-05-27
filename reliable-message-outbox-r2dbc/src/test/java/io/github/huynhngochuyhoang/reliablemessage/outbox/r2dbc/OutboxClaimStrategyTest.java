package io.github.huynhngochuyhoang.reliablemessage.outbox.r2dbc;

import io.r2dbc.spi.ConnectionFactories;
import org.junit.jupiter.api.Test;
import org.springframework.r2dbc.core.DatabaseClient;
import reactor.test.StepVerifier;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class OutboxClaimStrategyTest {

    @Test
    void unknownDialectUsesGenericStrategy() {
        OutboxSchema schema = new OutboxSchema("text", "text", "blob", "text", OutboxDatabaseDialect.GENERIC);

        OutboxClaimStrategy strategy = OutboxClaimStrategy.create(databaseClient(), schema, (row, metadata) -> null);

        assertThat(strategy).isInstanceOf(GenericConditionalUpdateOutboxClaimStrategy.class);
    }

    @Test
    void postgresqlDialectUsesPostgresqlStrategy() {
        OutboxSchema schema = new OutboxSchema("text", "text", "bytea", "text", OutboxDatabaseDialect.POSTGRESQL);

        OutboxClaimStrategy strategy = OutboxClaimStrategy.create(databaseClient(), schema, (row, metadata) -> null);

        assertThat(strategy).isInstanceOf(PostgresSkipLockedOutboxClaimStrategy.class);
    }

    @Test
    void postgresqlClaimSqlUsesSkipLockedAndReturning() {
        String sql = PostgresSkipLockedOutboxClaimStrategy.claimSql();

        assertThat(sql).containsIgnoringWhitespaces("for update skip locked");
        assertThat(sql).doesNotContain("row_number");
        assertThat(sql).doesNotContain(" over ");
        assertThat(sql).containsIgnoringWhitespaces("returning mo.id");
        assertThat(sql).containsIgnoringWhitespaces("order by claimed_created_at asc");
        assertThat(sql).containsIgnoringWhitespaces("limit :limit");
    }

    @Test
    void limitMustBePositive() {
        OutboxSchema schema = new OutboxSchema("text", "text", "blob", "text", OutboxDatabaseDialect.GENERIC);
        OutboxClaimStrategy strategy = OutboxClaimStrategy.create(databaseClient(), schema, (row, metadata) -> null);

        StepVerifier.create(strategy.claim(0, Instant.parse("2026-05-18T00:00:00Z")))
                .expectErrorMatches(error -> error instanceof IllegalArgumentException
                        && "limit must be positive".equals(error.getMessage()))
                .verify();
    }

    private static DatabaseClient databaseClient() {
        return DatabaseClient.create(ConnectionFactories.get(
                "r2dbc:h2:mem:///" + UUID.randomUUID() + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1"
        ));
    }
}
