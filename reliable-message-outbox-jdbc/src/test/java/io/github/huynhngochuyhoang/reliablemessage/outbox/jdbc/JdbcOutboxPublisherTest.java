package io.github.huynhngochuyhoang.reliablemessage.outbox.jdbc;

import io.github.huynhngochuyhoang.reliablemessage.core.PublishOptions;
import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessageHeaders;
import io.github.huynhngochuyhoang.reliablemessage.mvc.OutboxMessage;
import io.github.huynhngochuyhoang.reliablemessage.mvc.OutboxStore;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class JdbcOutboxPublisherTest {

    @Test
    void savesPendingOutboxMessageWithPublishOptions() {
        OutboxStore outboxStore = mock(OutboxStore.class);
        Instant now = Instant.parse("2026-05-18T00:00:00Z");
        JdbcOutboxPublisher publisher = new JdbcOutboxPublisher(
                outboxStore,
                Clock.fixed(now, ZoneOffset.UTC)
        );

        publisher.publishLater(
                "order.created",
                new OrderCreated("order-1"),
                PublishOptions.builder()
                        .aggregateId("order-1")
                        .idempotencyKey("event-1")
                        .correlationId("correlation-1")
                        .partitionKey("order-1")
                        .build()
        );

        ArgumentCaptor<OutboxMessage> message = ArgumentCaptor.forClass(OutboxMessage.class);
        verify(outboxStore).save(message.capture());
        assertEquals("order.created", message.getValue().eventName());
        assertEquals("order-1", message.getValue().aggregateId());
        assertEquals("event-1", message.getValue().idempotencyKey());
        assertEquals("order-1", message.getValue().partitionKey());
        assertEquals("correlation-1", message.getValue().headers().get(ReliableMessageHeaders.CORRELATION_ID));
        assertEquals(now, message.getValue().createdAt());
    }

    private record OrderCreated(String orderId) {
    }
}
