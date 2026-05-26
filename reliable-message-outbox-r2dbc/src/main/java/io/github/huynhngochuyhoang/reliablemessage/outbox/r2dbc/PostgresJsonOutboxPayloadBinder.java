package io.github.huynhngochuyhoang.reliablemessage.outbox.r2dbc;

import io.r2dbc.postgresql.codec.Json;
import org.springframework.r2dbc.core.DatabaseClient;

public class PostgresJsonOutboxPayloadBinder implements OutboxPayloadBinder {

    @Override
    public DatabaseClient.GenericExecuteSpec bindPayload(
            DatabaseClient.GenericExecuteSpec spec,
            String name,
            String jsonValue
    ) {
        return spec.bind(name, Json.of(jsonValue));
    }

    @Override
    public DatabaseClient.GenericExecuteSpec bindHeaders(
            DatabaseClient.GenericExecuteSpec spec,
            String name,
            String jsonValue
    ) {
        return spec.bind(name, Json.of(jsonValue));
    }
}
