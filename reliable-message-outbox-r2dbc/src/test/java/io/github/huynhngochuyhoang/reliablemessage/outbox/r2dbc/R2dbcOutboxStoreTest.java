package io.github.huynhngochuyhoang.reliablemessage.outbox.r2dbc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.huynhngochuyhoang.reliablemessage.core.MessageStatus;
import io.github.huynhngochuyhoang.reliablemessage.core.PublishOptions;
import io.github.huynhngochuyhoang.reliablemessage.webflux.OutboxMessage;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import org.junit.jupiter.api.Test;
import org.springframework.r2dbc.connection.R2dbcTransactionManager;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.test.StepVerifier;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;

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

    private static OutboxMessage message(String id) {
        return new OutboxMessage(
                id,
                "order.created",
                "order-1",
                "event-1",
                "order-1",
                new OrderCreated("order-1"),
                PublishOptions.builder().correlationId("correlation-1").build().headers(),
                MessageStatus.PENDING,
                0,
                null,
                NOW,
                null,
                null
        );
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

    private static TestStore store() {
        ConnectionFactory connectionFactory = ConnectionFactories.get(
                "r2dbc:h2:mem:///" + UUID.randomUUID() + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1"
        );
        DatabaseClient databaseClient = DatabaseClient.create(connectionFactory);
        Clock clock = Clock.fixed(NOW, ZoneOffset.UTC);
        return new TestStore(
                connectionFactory,
                databaseClient,
                new R2dbcOutboxStore(databaseClient, new ObjectMapper(), clock)
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
