package io.github.huynhngochuyhoang.reliablemessage.rabbit.webflux.bridge;

import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.rabbit.core.RabbitAdmin;

import java.util.Objects;

public class ReactiveRabbitBridgeTopologyAutoConfigurer {

    private final RabbitAdmin rabbitAdmin;
    private final RabbitWebFluxBridgeProperties properties;

    public ReactiveRabbitBridgeTopologyAutoConfigurer(RabbitAdmin rabbitAdmin, RabbitWebFluxBridgeProperties properties) {
        this.rabbitAdmin = rabbitAdmin;
        this.properties = Objects.requireNonNull(properties, "properties");
    }

    public void declareListenerTopology(String eventName, String queueName) {
        if (!properties.getRabbit().isAutoDeclare() || rabbitAdmin == null) {
            return;
        }

        DirectExchange exchange = new DirectExchange(properties.getRabbit().getExchange(), true, false);
        Queue queue = QueueBuilder.durable(queueName).build();

        rabbitAdmin.declareExchange(exchange);
        rabbitAdmin.declareQueue(queue);
        rabbitAdmin.declareBinding(BindingBuilder.bind(queue).to(exchange).with(eventName));
    }
}
