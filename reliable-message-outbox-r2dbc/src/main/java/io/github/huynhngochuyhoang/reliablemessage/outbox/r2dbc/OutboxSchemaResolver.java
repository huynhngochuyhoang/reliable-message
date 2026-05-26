package io.github.huynhngochuyhoang.reliablemessage.outbox.r2dbc;

import java.util.Objects;

public class OutboxSchemaResolver {

    private final R2dbcOutboxProperties properties;

    public OutboxSchemaResolver(R2dbcOutboxProperties properties) {
        this.properties = Objects.requireNonNull(properties, "properties must not be null");
    }

    public OutboxSchema resolve(OutboxDatabaseDialect dialect) {
        OutboxDatabaseDialect resolvedDialect = dialect == null ? OutboxDatabaseDialect.GENERIC : dialect;
        R2dbcOutboxProperties.Schema schemaProperties = properties.getSchema();
        DialectDefaults defaults = defaults(resolvedDialect);
        R2dbcOutboxProperties.PayloadStorage payloadStorage = schemaProperties.getPayloadStorage();
        if (payloadStorage == R2dbcOutboxProperties.PayloadStorage.BINARY) {
            throw new IllegalArgumentException("binary payload storage requires runtime codec/storage support that is not implemented yet");
        }

        return new OutboxSchema(
                configuredOrDefault(schemaProperties.getPayloadColumnType(), defaults.payloadType(payloadStorage)),
                configuredOrDefault(schemaProperties.getHeadersColumnType(), defaults.headersType(payloadStorage)),
                configuredOrDefault(schemaProperties.getPayloadBytesColumnType(), defaults.binaryType()),
                configuredOrDefault(schemaProperties.getLastErrorColumnType(), defaults.lastErrorType()),
                resolvedDialect
        );
    }

    private static String configuredOrDefault(String configured, String fallback) {
        return configured == null || configured.isBlank() ? fallback : configured;
    }

    private static DialectDefaults defaults(OutboxDatabaseDialect dialect) {
        return switch (dialect) {
            case POSTGRESQL -> new DialectDefaults("text", "jsonb", "bytea", "text");
            case MYSQL -> new DialectDefaults("longtext", "json", "longblob", "longtext");
            case ORACLE -> new DialectDefaults("clob", "clob", "blob", "clob");
            case SQL_SERVER -> new DialectDefaults("nvarchar(max)", "nvarchar(max)", "varbinary(max)", "nvarchar(max)");
            case GENERIC -> new DialectDefaults("text", "text", "blob", "text");
        };
    }

    private record DialectDefaults(
            String textType,
            String jsonType,
            String binaryType,
            String lastErrorType
    ) {

        String payloadType(R2dbcOutboxProperties.PayloadStorage payloadStorage) {
            return switch (payloadStorage) {
                case TEXT -> textType;
                case JSON -> jsonType;
                case BINARY -> textType;
            };
        }

        String headersType(R2dbcOutboxProperties.PayloadStorage payloadStorage) {
            return payloadStorage == R2dbcOutboxProperties.PayloadStorage.JSON ? jsonType : textType;
        }
    }
}
