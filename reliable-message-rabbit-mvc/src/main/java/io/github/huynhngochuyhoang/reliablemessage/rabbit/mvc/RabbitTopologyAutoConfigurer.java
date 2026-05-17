package io.github.huynhngochuyhoang.reliablemessage.rabbit.mvc;

import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.core.RabbitAdmin;

public class RabbitTopologyAutoConfigurer {

    private final RabbitAdmin rabbitAdmin;
    private final RabbitReliableMessageProperties properties;

    public RabbitTopologyAutoConfigurer(RabbitAdmin rabbitAdmin, RabbitReliableMessageProperties properties) {
        this.rabbitAdmin = rabbitAdmin;
        this.properties = properties;
    }

    public void declareListenerTopology(String eventName, String queueName) {
        if (!properties.getRabbit().isAutoDeclare() || rabbitAdmin == null) {
            return;
        }

        DirectExchange exchange = new DirectExchange(properties.getRabbit().getExchange(), true, false);
        Queue queue = new Queue(queueName, true);

        rabbitAdmin.declareExchange(exchange);
        rabbitAdmin.declareQueue(queue);
        rabbitAdmin.declareBinding(BindingBuilder.bind(queue).to(exchange).with(eventName));
    }
}
