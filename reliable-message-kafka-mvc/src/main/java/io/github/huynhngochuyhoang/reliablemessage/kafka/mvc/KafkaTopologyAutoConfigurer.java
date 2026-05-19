package io.github.huynhngochuyhoang.reliablemessage.kafka.mvc;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.ArrayList;
import java.util.List;

public class KafkaTopologyAutoConfigurer {

    private final KafkaAdmin kafkaAdmin;
    private final KafkaReliableMessageProperties properties;

    public KafkaTopologyAutoConfigurer(KafkaAdmin kafkaAdmin, KafkaReliableMessageProperties properties) {
        this.kafkaAdmin = kafkaAdmin;
        this.properties = properties;
    }

    public void declareListenerTopology(String topicName, String consumerGroup) {
        if (kafkaAdmin == null || !properties.getKafka().isAutoDeclare()) {
            return;
        }
        List<NewTopic> topics = new ArrayList<>();
        topics.add(newTopic(topicName));
        for (java.time.Duration delay : properties.getRetry().getBackoff()) {
            topics.add(newTopic(KafkaTopicNames.retryTopic(topicName, consumerGroup, delay)));
        }
        topics.add(newTopic(KafkaTopicNames.dltTopic(topicName, consumerGroup)));
        kafkaAdmin.createOrModifyTopics(topics.toArray(NewTopic[]::new));
    }

    private NewTopic newTopic(String topicName) {
        return new NewTopic(
                topicName,
                properties.getKafka().getPartitions(),
                properties.getKafka().getReplicationFactor()
        );
    }
}
