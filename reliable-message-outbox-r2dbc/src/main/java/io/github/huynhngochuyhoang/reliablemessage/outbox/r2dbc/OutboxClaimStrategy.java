package io.github.huynhngochuyhoang.reliablemessage.outbox.r2dbc;

import io.github.huynhngochuyhoang.reliablemessage.webflux.OutboxMessage;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import org.springframework.r2dbc.core.DatabaseClient;
import reactor.core.publisher.Flux;

import java.time.Instant;
import java.util.Objects;
import java.util.function.BiFunction;

public interface OutboxClaimStrategy {

    Flux<OutboxMessage> claim(int limit, Instant now);

    static OutboxClaimStrategy create(
            DatabaseClient databaseClient,
            OutboxSchema schema,
            BiFunction<Row, RowMetadata, OutboxMessage> rowMapper
    ) {
        Objects.requireNonNull(schema, "schema must not be null");
        if (schema.dialect() == OutboxDatabaseDialect.POSTGRESQL) {
            return new PostgresSkipLockedOutboxClaimStrategy(databaseClient, rowMapper);
        }
        return new GenericConditionalUpdateOutboxClaimStrategy(databaseClient, rowMapper);
    }
}
