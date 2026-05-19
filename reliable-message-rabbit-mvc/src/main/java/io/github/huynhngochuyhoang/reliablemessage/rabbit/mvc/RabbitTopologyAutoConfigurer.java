package io.github.huynhngochuyhoang.reliablemessage.rabbit.mvc;

import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.rabbit.core.RabbitAdmin;

import java.time.Duration;

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
        Queue queue = QueueBuilder.durable(queueName)
                .deadLetterExchange(properties.getRabbit().getExchange())
                .deadLetterRoutingKey(RabbitTopologyNames.dlqRoutingKey(queueName))
                .build();
        Queue dlq = QueueBuilder.durable(RabbitTopologyNames.dlqQueueName(queueName)).build();

        rabbitAdmin.declareExchange(exchange);
        rabbitAdmin.declareQueue(queue);
        rabbitAdmin.declareBinding(BindingBuilder.bind(queue).to(exchange).with(eventName));
        rabbitAdmin.declareQueue(dlq);
        rabbitAdmin.declareBinding(BindingBuilder.bind(dlq).to(exchange).with(RabbitTopologyNames.dlqRoutingKey(queueName)));

        for (Duration delay : properties.getRetry().getBackoff()) {
            Queue retryQueue = QueueBuilder.durable(RabbitTopologyNames.retryQueueName(queueName, delay))
                    .ttl(Math.toIntExact(delay.toMillis()))
                    .deadLetterExchange(properties.getRabbit().getExchange())
                    .deadLetterRoutingKey(queueName)
                    .build();
            rabbitAdmin.declareQueue(retryQueue);
            rabbitAdmin.declareBinding(BindingBuilder.bind(retryQueue)
                    .to(exchange)
                    .with(RabbitTopologyNames.retryRoutingKey(queueName, delay)));
        }
    }
}
