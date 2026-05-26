package io.github.huynhngochuyhoang.reliablemessage.outbox.r2dbc;

import java.util.Locale;
import java.util.Objects;

public record OutboxSchema(
        String payloadColumnType,
        String headersColumnType,
        String payloadBytesColumnType,
        String lastErrorColumnType,
        OutboxDatabaseDialect dialect,
        OutboxPayloadBinder payloadBinder
) {

    public OutboxSchema(String payloadColumnType, String headersColumnType, String payloadBytesColumnType, String lastErrorColumnType) {
        this(payloadColumnType, headersColumnType, payloadBytesColumnType, lastErrorColumnType, OutboxDatabaseDialect.GENERIC);
    }

    public OutboxSchema(String payloadColumnType, String headersColumnType, String payloadBytesColumnType, String lastErrorColumnType, OutboxDatabaseDialect dialect) {
        this(payloadColumnType, headersColumnType, payloadBytesColumnType, lastErrorColumnType, dialect,
                payloadBinder(dialect, payloadColumnType, headersColumnType));
    }

    public OutboxSchema {
        payloadColumnType = requireColumnType(payloadColumnType, "payloadColumnType");
        headersColumnType = requireColumnType(headersColumnType, "headersColumnType");
        payloadBytesColumnType = requireColumnType(payloadBytesColumnType, "payloadBytesColumnType");
        lastErrorColumnType = requireColumnType(lastErrorColumnType, "lastErrorColumnType");
        dialect = dialect == null ? OutboxDatabaseDialect.GENERIC : dialect;
        payloadBinder = Objects.requireNonNull(payloadBinder, "payloadBinder must not be null");
    }

    private static OutboxPayloadBinder payloadBinder(
            OutboxDatabaseDialect dialect,
            String payloadColumnType,
            String headersColumnType
    ) {
        OutboxDatabaseDialect resolvedDialect = dialect == null ? OutboxDatabaseDialect.GENERIC : dialect;
        if (resolvedDialect == OutboxDatabaseDialect.POSTGRESQL
                && (isJsonColumn(payloadColumnType) || isJsonColumn(headersColumnType))) {
            return new PostgresJsonOutboxPayloadBinder();
        }
        return new DefaultOutboxPayloadBinder();
    }

    private static boolean isJsonColumn(String columnType) {
        if (columnType == null) {
            return false;
        }
        String normalized = columnType.trim().toLowerCase(Locale.ROOT);
        return "json".equals(normalized) || "jsonb".equals(normalized);
    }

    private static String requireColumnType(String value, String name) {
        Objects.requireNonNull(value, name + " must not be null");
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value.trim();
    }
}
