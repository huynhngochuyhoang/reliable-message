package io.github.huynhngochuyhoang.reliablemessage.outbox.r2dbc;

import io.r2dbc.postgresql.codec.Json;
import org.springframework.r2dbc.core.DatabaseClient;

public class PostgresJsonOutboxPayloadBinder implements OutboxPayloadBinder {

    private final boolean payloadJson;
    private final boolean headersJson;

    public PostgresJsonOutboxPayloadBinder() {
        this(true, true);
    }

    public PostgresJsonOutboxPayloadBinder(boolean payloadJson, boolean headersJson) {
        this.payloadJson = payloadJson;
        this.headersJson = headersJson;
    }

    @Override
    public DatabaseClient.GenericExecuteSpec bindPayload(
            DatabaseClient.GenericExecuteSpec spec,
            String name,
            String jsonValue
    ) {
        return payloadJson ? spec.bind(name, Json.of(jsonValue)) : spec.bind(name, jsonValue);
    }

    @Override
    public DatabaseClient.GenericExecuteSpec bindHeaders(
            DatabaseClient.GenericExecuteSpec spec,
            String name,
            String jsonValue
    ) {
        return headersJson ? spec.bind(name, Json.of(jsonValue)) : spec.bind(name, jsonValue);
    }
}
