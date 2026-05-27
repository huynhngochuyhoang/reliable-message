package io.github.huynhngochuyhoang.reliablemessage.outbox.r2dbc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.huynhngochuyhoang.reliablemessage.core.MessageStatus;
import io.github.huynhngochuyhoang.reliablemessage.core.PublishOptions;
import io.github.huynhngochuyhoang.reliablemessage.webflux.OutboxMessage;
import io.r2dbc.postgresql.codec.Json;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import org.junit.jupiter.api.Test;
import org.springframework.r2dbc.connection.R2dbcTransactionManager;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.lang.reflect.Proxy;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class R2dbcOutboxStoreTest {

    private static final Instant NOW = Instant.parse("2026-05-18T00:00:00Z");

    @Test
    void savesOutboxRowInCallerReactiveTransaction() {
        TestStore testStore = store();
        TransactionalOperator transaction = TransactionalOperator.create(new R2dbcTransactionManager(testStore.connectionFactory));

        StepVerifier.create(testStore.store.initializeSchema()
                        .then(testStore.databaseClient.sql("create table orders (id varchar(64) primary key)")
                                .fetch()
                                .rowsUpdated()
                                .then())
                        .thenMany(transaction.execute(status ->
                                testStore.databaseClient.sql("insert into orders (id) values (:id)")
                                        .bind("id", "order-1")
                                        .fetch()
                                        .rowsUpdated()
                                        .then(testStore.store.save(message("event-1")))
                        ))
                        .then(count(testStore.databaseClient, "orders"))
                        .zipWith(count(testStore.databaseClient, "message_outbox")))
                .expectNextMatches(counts -> counts.getT1() == 1 && counts.getT2() == 1)
                .verifyComplete();
    }

    @Test
    void findsPendingRowsAndClaimsThem() {
        TestStore testStore = store();

        StepVerifier.create(testStore.store.initializeSchema()
                        .then(testStore.store.save(message("event-1")))
                        .thenMany(testStore.store.findPending(10))
                        .collectList())
                .expectNextMatches(messages ->
                        messages.size() == 1
                                && messages.getFirst().status() == MessageStatus.PROCESSING
                                && "order-1".equals(((JsonNode) messages.getFirst().payload()).get("orderId").asText()))
                .verifyComplete();

        StepVerifier.create(testStore.store.findPending(10).collectList())
                .expectNextMatches(messages -> messages.isEmpty())
                .verifyComplete();
    }

    @Test
    void marksClaimedRowsPublished() {
        TestStore testStore = store();

        StepVerifier.create(testStore.store.initializeSchema()
                        .then(testStore.store.save(message("event-1")))
                        .thenMany(testStore.store.findPending(10))
                        .then(testStore.store.markPublished("event-1"))
                        .then(status(testStore.databaseClient, "event-1")))
                .expectNext(MessageStatus.PUBLISHED.name())
                .verifyComplete();
    }

    @Test
    void mapsNullRetryCountAsZeroForLegacyRows() {
        TestStore testStore = store();

        StepVerifier.create(testStore.databaseClient.sql("""
                                create table message_outbox (
                                    id varchar(64) primary key,
                                    event_name varchar(255) not null,
                                    aggregate_id varchar(255),
                                    idempotency_key varchar(255),
                                    partition_key varchar(255),
                                    payload text not null,
                                    headers text,
                                    status varchar(32) not null,
                                    retry_count int,
                                    next_retry_at timestamp,
                                    processing_started_at timestamp,
                                    created_at timestamp not null,
                                    published_at timestamp,
                                    last_error text
                                )
                                """)
                        .fetch()
                        .rowsUpdated()
                        .then(testStore.databaseClient.sql("""
                                insert into message_outbox
                                (id, event_name, aggregate_id, idempotency_key, partition_key, payload, headers,
                                 status, retry_count, created_at)
                                values (:id, :eventName, :aggregateId, :idempotencyKey, :partitionKey, :payload, :headers,
                                        :status, :retryCount, :createdAt)
                                """)
                                .bind("id", "event-legacy")
                                .bind("eventName", "order.created")
                                .bind("aggregateId", "order-1")
                                .bind("idempotencyKey", "event-legacy")
                                .bind("partitionKey", "order-1")
                                .bind("payload", "{\"orderId\":\"order-1\"}")
                                .bind("headers", "{}")
                                .bind("status", MessageStatus.PENDING.name())
                                .bindNull("retryCount", Integer.class)
                                .bind("createdAt", NOW.atOffset(ZoneOffset.UTC).toLocalDateTime())
                                .fetch()
                                .rowsUpdated())
                        .thenMany(testStore.store.findPending(10))
                        .single())
                .expectNextMatches(message -> message.retryCount() == 0)
                .verifyComplete();
    }

    @Test
    void markFailedIncrementsNullRetryCountForLegacyRows() {
        TestStore testStore = store();

        StepVerifier.create(testStore.databaseClient.sql("""
                                create table message_outbox (
                                    id varchar(64) primary key,
                                    event_name varchar(255) not null,
                                    aggregate_id varchar(255),
                                    idempotency_key varchar(255),
                                    partition_key varchar(255),
                                    payload text not null,
                                    headers text,
                                    status varchar(32) not null,
                                    retry_count int,
                                    next_retry_at timestamp,
                                    processing_started_at timestamp,
                                    created_at timestamp not null,
                                    published_at timestamp,
                                    last_error text
                                )
                                """)
                        .fetch()
                        .rowsUpdated()
                        .then(testStore.databaseClient.sql("""
                                insert into message_outbox
                                (id, event_name, aggregate_id, idempotency_key, partition_key, payload, headers,
                                 status, retry_count, created_at)
                                values (:id, :eventName, :aggregateId, :idempotencyKey, :partitionKey, :payload, :headers,
                                        :status, :retryCount, :createdAt)
                                """)
                                .bind("id", "event-legacy")
                                .bind("eventName", "order.created")
                                .bind("aggregateId", "order-1")
                                .bind("idempotencyKey", "event-legacy")
                                .bind("partitionKey", "order-1")
                                .bind("payload", "{\"orderId\":\"order-1\"}")
                                .bind("headers", "{}")
                                .bind("status", MessageStatus.PENDING.name())
                                .bindNull("retryCount", Integer.class)
                                .bind("createdAt", NOW.atOffset(ZoneOffset.UTC).toLocalDateTime())
                                .fetch()
                                .rowsUpdated())
                        .thenMany(testStore.store.findPending(10))
                        .single()
                        .flatMap(message -> testStore.store.markFailed(message.id(), new IllegalStateException("publish failed"), NOW.plusSeconds(30)))
                        .then(retryCount(testStore.databaseClient, "event-legacy")))
                .expectNext(1)
                .verifyComplete();
    }

    @Test
    void postgresqlJsonModeUsesPostgresJsonPayloadBinder() {
        R2dbcOutboxProperties properties = new R2dbcOutboxProperties();
        properties.getSchema().setPayloadStorage(R2dbcOutboxProperties.PayloadStorage.JSON);
        OutboxSchema schema = new OutboxSchemaResolver(properties).resolve(OutboxDatabaseDialect.POSTGRESQL);

        assertThat(schema.payloadBinder()).isInstanceOf(PostgresJsonOutboxPayloadBinder.class);
    }

    @Test
    void postgresqlJsonBinderBindsOnlyJsonColumnsAsJson() {
        OutboxSchema schema = new OutboxSchema("text", "jsonb", "bytea", "text", OutboxDatabaseDialect.POSTGRESQL);
        Map<String, Object> boundValues = new HashMap<>();
        DatabaseClient.GenericExecuteSpec spec = recordingSpec(boundValues);

        spec = schema.payloadBinder().bindPayload(spec, "payload", "{\"orderId\":\"order-1\"}");
        schema.payloadBinder().bindHeaders(spec, "headers", "{\"traceId\":\"trace-1\"}");

        assertThat(boundValues.get("payload")).isInstanceOf(String.class);
        assertThat(boundValues.get("headers")).isInstanceOf(Json.class);
    }

    @Test
    void defaultSchemaUsesConnectionFactoryDialect() {
        DatabaseClient databaseClient = DatabaseClient.create(connectionFactory("PostgreSQL"));

        OutboxSchema schema = R2dbcOutboxStore.defaultSchema(databaseClient);

        assertThat(schema.dialect()).isEqualTo(OutboxDatabaseDialect.POSTGRESQL);
        assertThat(schema.payloadBytesColumnType()).isEqualTo("bytea");
    }

    @Test
    void postgresqlJsonModeInsertSqlStaysDialectNeutral() {
        String insertSql = R2dbcOutboxStore.insertSql();

        assertThat(insertSql).doesNotContain("cast(:payload as jsonb)");
        assertThat(insertSql).doesNotContain("::jsonb");
        assertThat(insertSql).contains(":payload");
        assertThat(insertSql).contains(":headers");
    }

    @Test
    void textModeUsesDefaultPayloadBinder() {
        OutboxSchema schema = new OutboxSchemaResolver(new R2dbcOutboxProperties()).resolve(OutboxDatabaseDialect.POSTGRESQL);

        assertThat(schema.payloadBinder()).isInstanceOf(DefaultOutboxPayloadBinder.class);
    }

    @Test
    void genericJsonFallbackUsesDefaultPayloadBinder() {
        R2dbcOutboxProperties properties = new R2dbcOutboxProperties();
        properties.getSchema().setPayloadStorage(R2dbcOutboxProperties.PayloadStorage.JSON);

        OutboxSchema schema = new OutboxSchemaResolver(properties).resolve(OutboxDatabaseDialect.GENERIC);

        assertThat(schema.payloadBinder()).isInstanceOf(DefaultOutboxPayloadBinder.class);
    }


    @Test
    void jsonHeadersSchemaSavesAndReadsPayloadAndHeaders() {
        TestStore testStore = store(new OutboxSchema("text", "json", "bytea", "text"));

        StepVerifier.create(testStore.store.initializeSchema()
                        .then(testStore.store.save(message("event-json")))
                        .thenMany(testStore.store.findPending(10))
                        .single())
                .assertNext(message -> {
                    assertThat(((JsonNode) message.payload()).get("orderId").asText()).isEqualTo("order-1");
                    assertThat(message.headers()).containsEntry("trace-id", "trace-1");
                })
                .verifyComplete();
    }

    @Test
    void postgresqlJsonbSchemaUsesPostgresBinderAndCleanSql() {
        OutboxSchema schema = new OutboxSchema("jsonb", "jsonb", "bytea", "text", OutboxDatabaseDialect.POSTGRESQL);

        assertThat(schema.payloadBinder()).isInstanceOf(PostgresJsonOutboxPayloadBinder.class);
        assertThat(R2dbcOutboxStore.insertSql()).doesNotContain("cast(:payload as jsonb)");
        assertThat(R2dbcOutboxStore.insertSql()).doesNotContain("::jsonb");
    }


    @Test
    void schemaInitializerUsesResolvedColumnTypes() {
        OutboxSchema schema = new OutboxSchema(
                "clob",
                "json",
                "blob",
                "clob"
        );

        String ddl = R2dbcOutboxStore.schemaDdl(schema);

        assertThat(ddl).contains("payload clob not null");
        assertThat(ddl).contains("headers json");
        assertThat(ddl).contains("payload_bytes blob");
        assertThat(ddl).contains("last_error clob");
    }

    @Test
    void schemaInitializerDoesNotHardcodePayloadTextOrJsonb() {
        OutboxSchema schema = new OutboxSchema(
                "longtext",
                "longtext",
                "longblob",
                "longtext"
        );

        String ddl = R2dbcOutboxStore.schemaDdl(schema);

        assertThat(ddl).contains("payload longtext not null");
        assertThat(ddl).doesNotContain("payload text not null");
        assertThat(ddl).doesNotContain("payload jsonb not null");
    }

    private static OutboxMessage message(String id) {
        return message(id, new OrderCreated("order-1"));
    }

    private static OutboxMessage message(String id, Object payload) {
        return new OutboxMessage(
                id,
                "order.created",
                "order-1",
                "event-1",
                "order-1",
                payload,
                PublishOptions.builder().correlationId("correlation-1").header("trace-id", "trace-1").build().headers(),
                MessageStatus.PENDING,
                0,
                null,
                NOW,
                null,
                null
        );
    }

    private static DatabaseClient.GenericExecuteSpec recordingSpec(Map<String, Object> boundValues) {
        Object[] proxy = new Object[1];
        proxy[0] = Proxy.newProxyInstance(
                DatabaseClient.GenericExecuteSpec.class.getClassLoader(),
                new Class<?>[]{DatabaseClient.GenericExecuteSpec.class},
                (ignored, method, args) -> {
                    if ("bind".equals(method.getName()) && args != null && args.length == 2 && args[0] instanceof String name) {
                        boundValues.put(name, args[1]);
                        return proxy[0];
                    }

                    if (method.getReturnType().isInstance(proxy[0])) {
                        return proxy[0];
                    }

                    throw new UnsupportedOperationException(method.getName());
                });
        return (DatabaseClient.GenericExecuteSpec) proxy[0];
    }

    private static ConnectionFactory connectionFactory(String name) {
        return new ConnectionFactory() {
            @Override
            public org.reactivestreams.Publisher<? extends Connection> create() {
                return Mono.error(new UnsupportedOperationException("not used"));
            }


            @Override
            public ConnectionFactoryMetadata getMetadata() {
                return () -> name;
            }

        };
    }

    private static reactor.core.publisher.Mono<Long> count(DatabaseClient databaseClient, String table) {
        return databaseClient.sql("select count(*) as row_count from " + table)
                .map((row, metadata) -> row.get("row_count", Long.class))
                .one();
    }

    private static reactor.core.publisher.Mono<String> status(DatabaseClient databaseClient, String id) {
        return databaseClient.sql("select status from message_outbox where id = :id")
                .bind("id", id)
                .map((row, metadata) -> row.get("status", String.class))
                .one();
    }

    private static reactor.core.publisher.Mono<Integer> retryCount(DatabaseClient databaseClient, String id) {
        return databaseClient.sql("select retry_count from message_outbox where id = :id")
                .bind("id", id)
                .map((row, metadata) -> row.get("retry_count", Integer.class))
                .one();
    }

    private static TestStore store() {
        return store(new OutboxSchemaResolver(new R2dbcOutboxProperties()).resolve(OutboxDatabaseDialect.POSTGRESQL));
    }

    private static TestStore store(OutboxSchema schema) {
        ConnectionFactory connectionFactory = ConnectionFactories.get(
                "r2dbc:h2:mem:///" + UUID.randomUUID() + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1"
        );
        DatabaseClient databaseClient = DatabaseClient.create(connectionFactory);
        Clock clock = Clock.fixed(NOW, ZoneOffset.UTC);
        return new TestStore(
                connectionFactory,
                databaseClient,
                new R2dbcOutboxStore(databaseClient, new ObjectMapper(), clock, schema)
        );
    }

    private record OrderCreated(String orderId) {
    }

    private record TestStore(
            ConnectionFactory connectionFactory,
            DatabaseClient databaseClient,
            R2dbcOutboxStore store
    ) {
    }
}
