package io.github.huynhngochuyhoang.reliablemessage.outbox.r2dbc;

import org.springframework.r2dbc.core.DatabaseClient;

public class DefaultOutboxPayloadBinder implements OutboxPayloadBinder {

    @Override
    public DatabaseClient.GenericExecuteSpec bindPayload(
            DatabaseClient.GenericExecuteSpec spec,
            String name,
            String jsonValue
    ) {
        return spec.bind(name, jsonValue);
    }

    @Override
    public DatabaseClient.GenericExecuteSpec bindHeaders(
            DatabaseClient.GenericExecuteSpec spec,
            String name,
            String jsonValue
    ) {
        return spec.bind(name, jsonValue);
    }
}
