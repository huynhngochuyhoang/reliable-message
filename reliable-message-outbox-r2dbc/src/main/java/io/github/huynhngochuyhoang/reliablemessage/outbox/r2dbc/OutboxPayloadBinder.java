package io.github.huynhngochuyhoang.reliablemessage.outbox.r2dbc;

import org.springframework.r2dbc.core.DatabaseClient;

public interface OutboxPayloadBinder {

    DatabaseClient.GenericExecuteSpec bindPayload(
            DatabaseClient.GenericExecuteSpec spec,
            String name,
            String jsonValue
    );

    DatabaseClient.GenericExecuteSpec bindHeaders(
            DatabaseClient.GenericExecuteSpec spec,
            String name,
            String jsonValue
    );
}
