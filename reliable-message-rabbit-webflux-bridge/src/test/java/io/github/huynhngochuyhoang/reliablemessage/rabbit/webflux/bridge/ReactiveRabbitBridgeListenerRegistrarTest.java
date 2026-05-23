package io.github.huynhngochuyhoang.reliablemessage.rabbit.webflux.bridge;

import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessage;
import io.github.huynhngochuyhoang.reliablemessage.core.serialization.MessageSerializer;
import io.github.huynhngochuyhoang.reliablemessage.webflux.ReactiveReliableListener;
import org.junit.jupiter.api.Test;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.Exchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.AbstractMessageListenerContainer;
import org.springframework.context.support.GenericApplicationContext;
import reactor.core.publisher.Mono;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ReactiveRabbitBridgeListenerRegistrarTest {

    @Test
    void registersReactiveReliableListenerMethodReturningMonoVoid() throws Exception {
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
            assertThat(prefetchCount(containers.getFirst())).isEqualTo(1);
        } finally {
            registrar.destroy();
            context.close();
        }
    }

    @Test
    void registersJdkProxiedListenerUsingTargetMethod() {
        RabbitWebFluxBridgeProperties properties = new RabbitWebFluxBridgeProperties();
        properties.setServiceName("orders");
        properties.getRabbit().setListenerAutoStartup(false);
        ReactiveRabbitBridgeListenerRegistrar registrar = new ReactiveRabbitBridgeListenerRegistrar(
                connectionFactory(),
                serializer(),
                properties
        );
        GenericApplicationContext context = new GenericApplicationContext();
        context.registerBean("listener", ListenerContract.class, () -> jdkProxy(new ProxiedListener()));
        context.refresh();

        try {
            registrar.setApplicationContext(context);
            registrar.afterSingletonsInstantiated();

            assertThat(registrar.endpoints()).hasSize(1);
            assertThat(registrar.endpoints().getFirst().eventName()).isEqualTo("order.created");
            assertThat(registrar.endpoints().getFirst().method().getDeclaringClass()).isEqualTo(ProxiedListener.class);
            assertThat(registrar.containers()).hasSize(1);
        } finally {
            registrar.destroy();
            context.close();
        }
    }

    @Test
    void declaresListenerQueueExchangeAndBinding() {
        RabbitWebFluxBridgeProperties properties = new RabbitWebFluxBridgeProperties();
        properties.setServiceName("orders");
        properties.getRabbit().setExchange("events.exchange");
        properties.getRabbit().setListenerAutoStartup(false);
        RecordingRabbitAdmin rabbitAdmin = new RecordingRabbitAdmin(connectionFactory());
        ReactiveRabbitBridgeListenerRegistrar registrar = new ReactiveRabbitBridgeListenerRegistrar(
                connectionFactory(),
                serializer(),
                properties,
                new ReactiveRabbitBridgeTopologyAutoConfigurer(rabbitAdmin, properties)
        );
        GenericApplicationContext context = new GenericApplicationContext();
        context.registerBean("listener", Listener.class, Listener::new);
        context.refresh();

        try {
            registrar.setApplicationContext(context);
            registrar.afterSingletonsInstantiated();

            assertThat(rabbitAdmin.exchanges()).extracting(Exchange::getName).containsExactly("events.exchange");
            assertThat(rabbitAdmin.queues()).extracting(Queue::getName).containsExactly("orders.order.created");
            assertThat(rabbitAdmin.bindings()).singleElement().satisfies(binding -> {
                assertThat(binding.getDestination()).isEqualTo("orders.order.created");
                assertThat(binding.getExchange()).isEqualTo("events.exchange");
                assertThat(binding.getRoutingKey()).isEqualTo("order.created");
            });
        } finally {
            registrar.destroy();
            context.close();
        }
    }

    @Test
    void skipsTopologyDeclarationWhenAutoDeclareIsDisabled() {
        RabbitWebFluxBridgeProperties properties = new RabbitWebFluxBridgeProperties();
        properties.getRabbit().setAutoDeclare(false);
        RecordingRabbitAdmin rabbitAdmin = new RecordingRabbitAdmin(connectionFactory());
        ReactiveRabbitBridgeTopologyAutoConfigurer topologyAutoConfigurer =
                new ReactiveRabbitBridgeTopologyAutoConfigurer(rabbitAdmin, properties);

        topologyAutoConfigurer.declareListenerTopology("order.created", "orders.order.created");

        assertThat(rabbitAdmin.exchanges()).isEmpty();
        assertThat(rabbitAdmin.queues()).isEmpty();
        assertThat(rabbitAdmin.bindings()).isEmpty();
    }

    private static ListenerContract jdkProxy(Object target) {
        ProxyFactory proxyFactory = new ProxyFactory(target);
        proxyFactory.setInterfaces(ListenerContract.class);
        proxyFactory.setProxyTargetClass(false);
        return (ListenerContract) proxyFactory.getProxy();
    }

    private static int prefetchCount(SimpleMessageListenerContainer container) throws Exception {
        Method method = AbstractMessageListenerContainer.class.getDeclaredMethod("getPrefetchCount");
        method.setAccessible(true);
        return (Integer) method.invoke(container);
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

    private static final class RecordingRabbitAdmin extends RabbitAdmin {
        private final List<Exchange> exchanges = new ArrayList<>();
        private final List<Queue> queues = new ArrayList<>();
        private final List<Binding> bindings = new ArrayList<>();

        private RecordingRabbitAdmin(ConnectionFactory connectionFactory) {
            super(connectionFactory);
        }

        @Override
        public void declareExchange(Exchange exchange) {
            exchanges.add(exchange);
        }

        @Override
        public String declareQueue(Queue queue) {
            queues.add(queue);
            return queue.getName();
        }

        @Override
        public void declareBinding(Binding binding) {
            bindings.add(binding);
        }

        List<Exchange> exchanges() {
            return exchanges;
        }

        List<Queue> queues() {
            return queues;
        }

        List<Binding> bindings() {
            return bindings;
        }
    }

    interface ListenerContract {
        Mono<Void> handle(ReliableMessage<OrderCreated> message);
    }

    static final class ProxiedListener implements ListenerContract {
        @Override
        @ReactiveReliableListener("order.created")
        public Mono<Void> handle(ReliableMessage<OrderCreated> message) {
            return Mono.empty();
        }
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
