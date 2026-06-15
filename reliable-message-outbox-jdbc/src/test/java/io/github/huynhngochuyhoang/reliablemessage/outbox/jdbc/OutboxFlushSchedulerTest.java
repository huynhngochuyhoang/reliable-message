package io.github.huynhngochuyhoang.reliablemessage.outbox.jdbc;

import io.github.huynhngochuyhoang.reliablemessage.core.MessageStatus;
import io.github.huynhngochuyhoang.reliablemessage.core.PublishOptions;
import io.github.huynhngochuyhoang.reliablemessage.mvc.OutboxMessage;
import io.github.huynhngochuyhoang.reliablemessage.mvc.OutboxStore;
import io.github.huynhngochuyhoang.reliablemessage.mvc.ReliablePublisher;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class OutboxFlushSchedulerTest {

    private static final Instant NOW = Instant.parse("2026-05-18T00:00:00Z");

    @Test
    void publishesPendingRowsAndMarksThemPublished() {
        OutboxStore outboxStore = mock(OutboxStore.class);
        ReliablePublisher publisher = mock(ReliablePublisher.class);
        when(outboxStore.findPending(100)).thenReturn(List.of(message()));

        OutboxFlushScheduler scheduler = scheduler(outboxStore, publisher);

        assertEquals(1, scheduler.flushBatch());
        verify(publisher).publish(eq("order.created"), eq("payload"), any(PublishOptions.class));
        verify(outboxStore).markPublished("event-1");
    }

    @Test
    void doesNotPublishAgainWhenStoreHasNoPendingRowsAfterSuccess() {
        OutboxStore outboxStore = mock(OutboxStore.class);
        ReliablePublisher publisher = mock(ReliablePublisher.class);
        when(outboxStore.findPending(100))
                .thenReturn(List.of(message()))
                .thenReturn(List.of());

        OutboxFlushScheduler scheduler = scheduler(outboxStore, publisher);

        assertEquals(1, scheduler.flushBatch());
        assertEquals(0, scheduler.flushBatch());
        verify(publisher).publish(eq("order.created"), eq("payload"), any(PublishOptions.class));
        verify(outboxStore).markPublished("event-1");
    }

    @Test
    void failedPublishIsMarkedForRetry() {
        OutboxStore outboxStore = mock(OutboxStore.class);
        ReliablePublisher publisher = mock(ReliablePublisher.class);
        when(outboxStore.findPending(100)).thenReturn(List.of(message()));
        doThrow(new IllegalStateException("broker down"))
                .when(publisher)
                .publish(eq("order.created"), eq("payload"), any(PublishOptions.class));

        OutboxFlushScheduler scheduler = scheduler(outboxStore, publisher);

        assertEquals(0, scheduler.flushBatch());
        verify(outboxStore, never()).markPublished("event-1");

        ArgumentCaptor<Instant> nextRetryAt = ArgumentCaptor.forClass(Instant.class);
        verify(outboxStore).markFailed(eq("event-1"), any(IllegalStateException.class), nextRetryAt.capture());
        assertEquals(NOW.plusSeconds(30), nextRetryAt.getValue());
    }

    private static OutboxFlushScheduler scheduler(OutboxStore outboxStore, ReliablePublisher publisher) {
        JdbcOutboxProperties properties = new JdbcOutboxProperties();
        properties.setRetryDelay(Duration.ofSeconds(30));
        return new OutboxFlushScheduler(
                outboxStore,
                publisher,
                properties,
                Clock.fixed(NOW, ZoneOffset.UTC)
        );
    }

    private static OutboxMessage message() {
        return new OutboxMessage(
                "event-1",
                "order.created",
                "order-1",
                "event-1",
                "order-1",
                "payload",
                null,
                MessageStatus.PENDING,
                0,
                null,
                NOW,
                null,
                null
        );
    }
}
