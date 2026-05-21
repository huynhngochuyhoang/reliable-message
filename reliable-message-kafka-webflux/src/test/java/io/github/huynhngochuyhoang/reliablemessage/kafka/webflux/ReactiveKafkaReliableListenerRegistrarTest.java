package io.github.huynhngochuyhoang.reliablemessage.kafka.webflux;

import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessage;
import io.github.huynhngochuyhoang.reliablemessage.webflux.ReactiveReliableListener;
import org.junit.jupiter.api.Test;
import org.springframework.context.support.StaticApplicationContext;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

class ReactiveKafkaReliableListenerRegistrarTest {

    @Test
    void createsContainersWithConfiguredBackpressureSettings() {
        ReactiveKafkaReliableMessageProperties properties = new ReactiveKafkaReliableMessageProperties();
        properties.getKafka().setListenerAutoStartup(false);
        properties.getKafka().setTopicPrefix("app.");
        properties.getKafka().setConsumerGroup("order-service");
        properties.getReactive().setMaxConcurrency(16);
        properties.getReactive().setPrefetch(32);
        CapturingReceiverFactory receiverFactory = new CapturingReceiverFactory();
        StaticApplicationContext context = new StaticApplicationContext();
        context.registerSingleton("listener", TestListener.class);
        context.refresh();

        ReactiveKafkaReliableListenerRegistrar registrar = new ReactiveKafkaReliableListenerRegistrar(
                receiverFactory,
                new JacksonReliableMessageSerializer(new com.fasterxml.jackson.databind.ObjectMapper()),
                properties,
                null,
                null
        );
        registrar.setApplicationContext(context);
        registrar.afterSingletonsInstantiated();

        assertEquals(1, registrar.containers().size());
        assertEquals(16, registrar.containers().getFirst().maxConcurrency());
        assertEquals(32, registrar.containers().getFirst().prefetch());
        assertEquals("order-service", receiverFactory.consumerGroup);
        assertEquals("app.order.created", receiverFactory.topics.getFirst());
        assertEquals("app.order.created.order-service.retry.5s", receiverFactory.topics.get(1));
    }

    static final class TestListener {

        @ReactiveReliableListener("order.created")
        Mono<Void> handle(ReliableMessage<OrderCreated> message) {
            return Mono.empty();
        }
    }

    record OrderCreated(String orderId) {
    }

    static final class CapturingReceiverFactory implements ReactiveKafkaReceiverFactory {
        private List<String> topics = new ArrayList<>();
        private String consumerGroup;

        @Override
        @SuppressWarnings("unchecked")
        public KafkaReceiver<String, byte[]> create(List<String> topics, String consumerGroup) {
            this.topics = topics;
            this.consumerGroup = consumerGroup;
            KafkaReceiver<String, byte[]> receiver = org.mockito.Mockito.mock(KafkaReceiver.class);
            when(receiver.receive()).thenReturn(Flux.never());
            return receiver;
        }
    }
}
