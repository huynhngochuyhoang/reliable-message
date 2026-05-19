package io.github.huynhngochuyhoang.reliablemessage.kafka.webflux;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.huynhngochuyhoang.reliablemessage.core.PublishOptions;
import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessageHeaders;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;
import reactor.test.StepVerifier;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

class ReactiveKafkaReliablePublisherTest {

    @Test
    void publishesSerializedReliableMessage() {
        AtomicReference<ProducerRecord<String, byte[]>> recordRef = new AtomicReference<>();
        KafkaSender<String, byte[]> kafkaSender = kafkaSender(recordRef);
        ReactiveKafkaReliableMessageProperties properties = new ReactiveKafkaReliableMessageProperties();
        properties.getKafka().setTopicPrefix("app.");
        ReactiveKafkaReliablePublisher publisher = new ReactiveKafkaReliablePublisher(
                kafkaSender,
                new JacksonReliableMessageSerializer(new ObjectMapper()),
                properties,
                Clock.fixed(Instant.parse("2026-05-18T00:00:00Z"), ZoneOffset.UTC)
        );

        StepVerifier.create(publisher.publish(
                        "order.created",
                        new OrderCreated("order-1"),
                        PublishOptions.builder()
                                .partitionKey("order-1")
                                .correlationId("correlation-1")
                                .build()
                ))
                .verifyComplete();

        ProducerRecord<String, byte[]> record = recordRef.get();
        assertEquals("app.order.created", record.topic());
        assertEquals("order-1", record.key());
        assertEquals("correlation-1", header(record, ReliableMessageHeaders.CORRELATION_ID));
        assertEquals("order.created", header(record, ReliableMessageHeaders.EVENT_NAME));
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

    private static String header(ProducerRecord<String, byte[]> record, String name) {
        org.apache.kafka.common.header.Header header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), java.nio.charset.StandardCharsets.UTF_8);
    }

    record OrderCreated(String orderId) {
    }
}
