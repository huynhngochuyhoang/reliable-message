package io.github.huynhngochuyhoang.reliablemessage.kafka.mvc;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.huynhngochuyhoang.reliablemessage.core.PublishOptions;
import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessage;
import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessageHeaders;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class KafkaReliablePublisherTest {

    @Test
    void publishesEnvelopeToPrefixedTopicWithPartitionKey() {
        KafkaTemplate<String, byte[]> kafkaTemplate = kafkaTemplate();
        KafkaReliableMessageProperties properties = new KafkaReliableMessageProperties();
        properties.getKafka().setTopicPrefix("app.");
        JacksonReliableMessageSerializer serializer = new JacksonReliableMessageSerializer(new ObjectMapper());
        KafkaReliablePublisher publisher = new KafkaReliablePublisher(
                kafkaTemplate,
                serializer,
                properties,
                Clock.fixed(Instant.parse("2026-05-17T00:00:00Z"), ZoneOffset.UTC),
                new SimpleMeterRegistry()
        );

        publisher.publish(
                "order.created",
                new OrderCreated("order-1"),
                PublishOptions.builder()
                        .aggregateId("order-1")
                        .idempotencyKey("event-1")
                        .correlationId("correlation-1")
                        .partitionKey("order-1")
                        .build()
        );

        ArgumentCaptor<ProducerRecord<String, byte[]>> recordCaptor = producerRecordCaptor();
        verify(kafkaTemplate).send(recordCaptor.capture());

        ProducerRecord<String, byte[]> record = recordCaptor.getValue();
        ReliableMessage<OrderCreated> envelope = serializer.deserialize(record.value(), OrderCreated.class);

        assertEquals("app.order.created", record.topic());
        assertEquals("order-1", record.key());
        assertEquals("order.created", envelope.eventName());
        assertEquals("order-1", envelope.payload().orderId());
        assertEquals("correlation-1", envelope.correlationId());
        assertEquals("correlation-1", header(record, ReliableMessageHeaders.CORRELATION_ID));
        assertNotNull(header(record, ReliableMessageHeaders.MESSAGE_ID));
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

    record OrderCreated(String orderId) {
    }
}
