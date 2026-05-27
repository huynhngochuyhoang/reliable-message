package io.github.huynhngochuyhoang.reliablemessage.outbox.r2dbc;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OutboxSchemaResolverTest {

    @Test
    void explicitPayloadColumnTypeOverridesDialectDefault() {
        R2dbcOutboxProperties properties = properties();
        properties.getSchema().setPayloadStorage(R2dbcOutboxProperties.PayloadStorage.JSON);
        properties.getSchema().setPayloadColumnType("json");

        OutboxSchema schema = new OutboxSchemaResolver(properties).resolve(OutboxDatabaseDialect.POSTGRESQL);

        assertThat(schema.payloadColumnType()).isEqualTo("json");
    }

    @Test
    void explicitHeadersColumnTypeOverridesDialectDefault() {
        R2dbcOutboxProperties properties = properties();
        properties.getSchema().setHeadersColumnType("json");

        OutboxSchema schema = new OutboxSchemaResolver(properties).resolve(OutboxDatabaseDialect.POSTGRESQL);

        assertThat(schema.headersColumnType()).isEqualTo("json");
    }

    @Test
    void explicitPayloadBytesColumnTypeOverridesDialectDefault() {
        R2dbcOutboxProperties properties = properties();
        properties.getSchema().setPayloadBytesColumnType("blob");

        OutboxSchema schema = new OutboxSchemaResolver(properties).resolve(OutboxDatabaseDialect.POSTGRESQL);

        assertThat(schema.payloadBytesColumnType()).isEqualTo("blob");
    }

    @Test
    void postgresqlJsonModeResolvesPayloadAndHeadersToJsonb() {
        R2dbcOutboxProperties properties = properties();
        properties.getSchema().setPayloadStorage(R2dbcOutboxProperties.PayloadStorage.JSON);

        OutboxSchema schema = new OutboxSchemaResolver(properties).resolve(OutboxDatabaseDialect.POSTGRESQL);

        assertThat(schema.payloadColumnType()).isEqualTo("jsonb");
        assertThat(schema.headersColumnType()).isEqualTo("jsonb");
    }


    @Test
    void mysqlTextModeResolvesPayloadToLongtext() {
        R2dbcOutboxProperties properties = properties();

        OutboxSchema schema = new OutboxSchemaResolver(properties).resolve(OutboxDatabaseDialect.MYSQL);

        assertThat(schema.payloadColumnType()).isEqualTo("longtext");
    }


    @Test
    void oracleTextModeResolvesPayloadToClob() {
        R2dbcOutboxProperties properties = properties();

        OutboxSchema schema = new OutboxSchemaResolver(properties).resolve(OutboxDatabaseDialect.ORACLE);

        assertThat(schema.payloadColumnType()).isEqualTo("clob");
    }

    @Test
    void sqlServerTextModeResolvesPayloadToNvarcharMax() {
        R2dbcOutboxProperties properties = properties();

        OutboxSchema schema = new OutboxSchemaResolver(properties).resolve(OutboxDatabaseDialect.SQL_SERVER);

        assertThat(schema.payloadColumnType()).isEqualTo("nvarchar(max)");
    }

    @Test
    void unknownDialectJsonModeFallsBackToGenericText() {
        R2dbcOutboxProperties properties = properties();
        properties.getSchema().setPayloadStorage(R2dbcOutboxProperties.PayloadStorage.JSON);

        OutboxSchema schema = new OutboxSchemaResolver(properties).resolve(OutboxDatabaseDialect.GENERIC);

        assertThat(schema.payloadColumnType()).isEqualTo("text");
        assertThat(schema.headersColumnType()).isEqualTo("text");
    }

    @Test
    void binaryPayloadStorageFailsClearly() {
        R2dbcOutboxProperties properties = properties();
        properties.getSchema().setPayloadStorage(R2dbcOutboxProperties.PayloadStorage.BINARY);

        assertThatThrownBy(() -> new OutboxSchemaResolver(properties).resolve(OutboxDatabaseDialect.POSTGRESQL))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("binary payload storage requires runtime codec/storage support that is not implemented yet");
    }

    @Test
    void invalidPayloadStorageFailsClearly() {
        R2dbcOutboxProperties properties = properties();

        assertThatThrownBy(() -> properties.getSchema().setPayloadStorage(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("payloadStorage must not be null");
    }

    private static R2dbcOutboxProperties properties() {
        return new R2dbcOutboxProperties();
    }
}
