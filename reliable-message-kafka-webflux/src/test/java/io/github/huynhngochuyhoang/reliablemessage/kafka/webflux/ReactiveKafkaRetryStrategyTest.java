package io.github.huynhngochuyhoang.reliablemessage.kafka.webflux;

import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessageHeaders;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;
import reactor.test.StepVerifier;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

class ReactiveKafkaRetryStrategyTest {

    @Test
    void routesToRetryTopicBeforeAttemptsAreExhausted() {
        AtomicReference<ProducerRecord<String, byte[]>> recordRef = new AtomicReference<>();
        ReactiveKafkaRetryStrategy retryStrategy = retryStrategy(recordRef);

        StepVerifier.create(retryStrategy.routeFailure(record(0), endpoint(), new IllegalStateException("boom")))
                .verifyComplete();

        ProducerRecord<String, byte[]> routed = recordRef.get();
        assertEquals("app.order.created.order-service.retry.5s", routed.topic());
        assertEquals("1", header(routed.headers(), ReliableMessageHeaders.RETRY_COUNT));
        assertEquals(
                "2026-05-18T00:00:05Z",
                Instant.ofEpochMilli(Long.parseLong(header(routed.headers(), ReliableMessageHeaders.RETRY_NOT_BEFORE))).toString()
        );
        assertEquals("boom", header(routed.headers(), "x-error-message"));
    }

    @Test
    void routesToDltWhenAttemptsAreExhausted() {
        AtomicReference<ProducerRecord<String, byte[]>> recordRef = new AtomicReference<>();
        ReactiveKafkaRetryStrategy retryStrategy = retryStrategy(recordRef);

        StepVerifier.create(retryStrategy.routeFailure(record(4), endpoint(), new IllegalStateException("boom")))
                .verifyComplete();

        ProducerRecord<String, byte[]> routed = recordRef.get();
        assertEquals("app.order.created.order-service.dlt", routed.topic());
        assertEquals("5", header(routed.headers(), ReliableMessageHeaders.RETRY_COUNT));
        assertEquals(null, header(routed.headers(), ReliableMessageHeaders.RETRY_NOT_BEFORE));
    }

    private static ReactiveKafkaRetryStrategy retryStrategy(AtomicReference<ProducerRecord<String, byte[]>> recordRef) {
        return new ReactiveKafkaRetryStrategy(
                kafkaSender(recordRef),
                new ReactiveKafkaReliableMessageProperties(),
                Clock.fixed(Instant.parse("2026-05-18T00:00:00Z"), ZoneOffset.UTC)
        );
    }

    private static ReactiveKafkaReliableListenerEndpoint endpoint() {
        return new ReactiveKafkaReliableListenerEndpoint(
                "listener",
                new Object(),
                Object.class.getDeclaredMethods()[0],
                "order.created",
                "app.order.created",
                "order-service",
                Object.class
        );
    }

    private static TestRecord record(int retryCount) {
        RecordHeaders headers = new RecordHeaders();
        headers.add(ReliableMessageHeaders.MESSAGE_ID, "message-1".getBytes(StandardCharsets.UTF_8));
        if (retryCount > 0) {
            headers.add(ReliableMessageHeaders.RETRY_COUNT, String.valueOf(retryCount).getBytes(StandardCharsets.UTF_8));
        }
        return new TestRecord(headers);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static KafkaSender<String, byte[]> kafkaSender(AtomicReference<ProducerRecord<String, byte[]>> recordRef) {
        KafkaSender<String, byte[]> kafkaSender = org.mockito.Mockito.mock(KafkaSender.class);
        when(kafkaSender.send(any(org.reactivestreams.Publisher.class))).thenAnswer(invocation -> {
            org.reactivestreams.Publisher<SenderRecord<String, byte[], String>> publisher = invocation.getArgument(0);
            return Flux.from(publisher).map(record -> {
                recordRef.set(record);
                SenderResult<String> result = org.mockito.Mockito.mock(SenderResult.class);
                when(result.correlationMetadata()).thenReturn(record.correlationMetadata());
                return result;
            });
        });
        return kafkaSender;
    }

    private static String header(Headers headers, String name) {
        org.apache.kafka.common.header.Header header = headers.lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    private record TestRecord(Headers headers) implements ReactiveKafkaReceivedRecord {
        @Override
        public String topic() {
            return "app.order.created";
        }

        @Override
        public String key() {
            return "order-1";
        }

        @Override
        public byte[] value() {
            return "{}".getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public ReactiveKafkaReceiverOffset receiverOffset() {
            return () -> Mono.empty();
        }
    }
}
