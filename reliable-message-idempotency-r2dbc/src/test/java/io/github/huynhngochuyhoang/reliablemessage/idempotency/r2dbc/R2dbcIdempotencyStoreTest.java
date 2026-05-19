package io.github.huynhngochuyhoang.reliablemessage.idempotency.r2dbc;

import io.github.huynhngochuyhoang.reliablemessage.webflux.IdempotencyState;
import io.r2dbc.spi.ConnectionFactories;
import org.junit.jupiter.api.Test;
import org.springframework.r2dbc.core.DatabaseClient;
import reactor.test.StepVerifier;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;

class R2dbcIdempotencyStoreTest {

    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-05-18T00:00:00Z"), ZoneOffset.UTC);

    @Test
    void startsOnceAndTreatsProcessingKeyAsDuplicate() {
        R2dbcIdempotencyStore store = store();

        StepVerifier.create(store.initializeSchema()
                        .then(store.tryStart("event-1", Duration.ofMinutes(5)))
                        .flatMap(first -> store.tryStart("event-1", Duration.ofMinutes(5))
                                .map(second -> first.started()
                                        && !second.started()
                                        && second.state() == IdempotencyState.PROCESSING)))
                .expectNext(true)
                .verifyComplete();
    }

    @Test
    void successfulKeyIsDuplicate() {
        R2dbcIdempotencyStore store = store();

        StepVerifier.create(store.initializeSchema()
                        .then(store.tryStart("event-1", Duration.ofMinutes(5)))
                        .then(store.markSuccess("event-1"))
                        .then(store.tryStart("event-1", Duration.ofMinutes(5))))
                .expectNextMatches(result -> !result.started() && result.state() == IdempotencyState.SUCCESS)
                .verifyComplete();
    }

    @Test
    void failedKeyCanStartAgain() {
        R2dbcIdempotencyStore store = store();

        StepVerifier.create(store.initializeSchema()
                        .then(store.tryStart("event-1", Duration.ofMinutes(5)))
                        .then(store.markFailed("event-1", new IllegalStateException("boom")))
                        .then(store.tryStart("event-1", Duration.ofMinutes(5))))
                .expectNextMatches(result -> result.started() && result.state() == IdempotencyState.PROCESSING)
                .verifyComplete();
    }

    private static R2dbcIdempotencyStore store() {
        return new R2dbcIdempotencyStore(
                DatabaseClient.create(ConnectionFactories.get(
                        "r2dbc:h2:mem:///" + UUID.randomUUID() + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1"
                )),
                CLOCK
        );
    }
}
