package io.github.huynhngochuyhoang.reliablemessage.rabbit.webflux.bridge;

import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessage;
import io.github.huynhngochuyhoang.reliablemessage.core.serialization.MessageSerializer;
import io.github.huynhngochuyhoang.reliablemessage.webflux.ReactiveReliableListener;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.context.support.GenericApplicationContext;
import reactor.core.publisher.Mono;

import java.lang.reflect.Proxy;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ReactiveRabbitBridgeListenerRegistrarTest {

    @Test
    void registersReactiveReliableListenerMethodReturningMonoVoid() {
        RabbitWebFluxBridgeProperties properties = new RabbitWebFluxBridgeProperties();
        properties.setServiceName("orders");
        properties.getRabbit().setListenerAutoStartup(false);
        ReactiveRabbitBridgeListenerRegistrar registrar = new ReactiveRabbitBridgeListenerRegistrar(
                connectionFactory(),
                serializer(),
                properties
        );
        GenericApplicationContext context = new GenericApplicationContext();
        context.registerBean("listener", Listener.class, Listener::new);
        context.refresh();

        try {
            registrar.setApplicationContext(context);
            registrar.afterSingletonsInstantiated();

            List<ReactiveRabbitBridgeListenerEndpoint> endpoints = registrar.endpoints();
            List<SimpleMessageListenerContainer> containers = registrar.containers();
            assertThat(endpoints).hasSize(1);
            assertThat(endpoints.getFirst().eventName()).isEqualTo("order.created");
            assertThat(endpoints.getFirst().queueName()).isEqualTo("orders.order.created");
            assertThat(containers).hasSize(1);
            assertThat(containers.getFirst().getQueueNames()).containsExactly("orders.order.created");
            assertThat(containers.getFirst().isAutoStartup()).isFalse();
            assertThat(containers.getFirst().getMessageListener()).isInstanceOf(ReactiveRabbitBridgeMessageHandler.class);
        } finally {
            registrar.destroy();
            context.close();
        }
    }

    private static ConnectionFactory connectionFactory() {
        return (ConnectionFactory) Proxy.newProxyInstance(
                ConnectionFactory.class.getClassLoader(),
                new Class<?>[]{ConnectionFactory.class},
                (proxy, method, args) -> null
        );
    }

    private static MessageSerializer serializer() {
        return new MessageSerializer() {
            @Override
            public <T> byte[] serialize(ReliableMessage<T> message) {
                return new byte[]{1};
            }

            @Override
            public <T> ReliableMessage<T> deserialize(byte[] content, Class<T> payloadType) {
                throw new UnsupportedOperationException("deserialize is not used by registrar tests");
            }
        };
    }

    static final class Listener {
        @ReactiveReliableListener("order.created")
        Mono<Void> handle(ReliableMessage<OrderCreated> message) {
            return Mono.empty();
        }
    }

    record OrderCreated(String orderId) {
    }
}
