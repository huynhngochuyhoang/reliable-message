package io.github.huynhngochuyhoang.reliablemessage.kafka.mvc;

import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessageHeaders;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class KafkaRetryStrategyTest {

    @Test
    void routesToRetryTopicBeforeAttemptsAreExhausted() throws Exception {
        KafkaTemplate<String, byte[]> kafkaTemplate = kafkaTemplate();
        KafkaRetryStrategy retryStrategy = new KafkaRetryStrategy(
                kafkaTemplate,
                new KafkaReliableMessageProperties(),
                new SimpleMeterRegistry()
        );

        retryStrategy.routeFailure(record(0), endpoint(), new IllegalStateException("boom"));

        ArgumentCaptor<ProducerRecord<String, byte[]>> recordCaptor = producerRecordCaptor();
        verify(kafkaTemplate).send(recordCaptor.capture());
        ProducerRecord<String, byte[]> routed = recordCaptor.getValue();

        assertEquals("app.order.created.order-service.retry.5s", routed.topic());
        assertEquals("order-1", routed.key());
        assertEquals("1", header(routed, ReliableMessageHeaders.RETRY_COUNT));
        assertEquals("boom", header(routed, "x-error-message"));
    }

    @Test
    void routesToDltWhenAttemptsAreExhausted() throws Exception {
        KafkaTemplate<String, byte[]> kafkaTemplate = kafkaTemplate();
        KafkaRetryStrategy retryStrategy = new KafkaRetryStrategy(
                kafkaTemplate,
                new KafkaReliableMessageProperties(),
                new SimpleMeterRegistry()
        );

        retryStrategy.routeFailure(record(4), endpoint(), new IllegalStateException("boom"));

        ArgumentCaptor<ProducerRecord<String, byte[]>> recordCaptor = producerRecordCaptor();
        verify(kafkaTemplate).send(recordCaptor.capture());

        assertEquals("app.order.created.order-service.dlt", recordCaptor.getValue().topic());
        assertEquals("5", header(recordCaptor.getValue(), ReliableMessageHeaders.RETRY_COUNT));
    }

    private static KafkaReliableListenerEndpoint endpoint() throws NoSuchMethodException {
        Method method = Listener.class.getDeclaredMethod("handle", io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessage.class);
        Listener listener = new Listener();
        return new KafkaReliableListenerEndpoint(
                "listener",
                listener,
                method,
                "order.created",
                "app.order.created",
                "order-service",
                Object.class
        );
    }

    private static ConsumerRecord<String, byte[]> record(int retryCount) {
        ConsumerRecord<String, byte[]> record = new ConsumerRecord<>(
                "app.order.created",
                0,
                42L,
                "order-1",
                "{}".getBytes(StandardCharsets.UTF_8)
        );
        if (retryCount > 0) {
            record.headers().add(
                    ReliableMessageHeaders.RETRY_COUNT,
                    String.valueOf(retryCount).getBytes(StandardCharsets.UTF_8)
            );
        }
        record.headers().add(ReliableMessageHeaders.MESSAGE_ID, "message-1".getBytes(StandardCharsets.UTF_8));
        return record;
    }

    @SuppressWarnings("unchecked")
    private static KafkaTemplate<String, byte[]> kafkaTemplate() {
        KafkaTemplate<String, byte[]> kafkaTemplate = org.mockito.Mockito.mock(KafkaTemplate.class);
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.completedFuture(null));
        return kafkaTemplate;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static ArgumentCaptor<ProducerRecord<String, byte[]>> producerRecordCaptor() {
        return (ArgumentCaptor) ArgumentCaptor.forClass(ProducerRecord.class);
    }

    private static String header(ProducerRecord<String, byte[]> record, String name) {
        return new String(record.headers().lastHeader(name).value(), StandardCharsets.UTF_8);
    }

    static final class Listener {
        void handle(io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessage<Object> message) {
        }
    }
}
