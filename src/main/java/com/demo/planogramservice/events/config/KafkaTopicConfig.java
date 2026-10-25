package com.demo.planogramservice.events.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    public static final String PING_TOPIC = "demo-ping-topic";
    public static final String TRANSACTION_TOPIC = "transaction-events";
    public static final String FRAUD_DETECTION_TOPIC = "fraud-detection-topic";
    public static final String ACCOUNT_COUNT_TOPIC = "account-count-topic";
    public static final String ORDER_TOPIC = "order-events";
    public static final String ORDER_DLT_TOPIC = "order-events.DLT";

    @Bean
    public NewTopic transactionTopic() {
        return TopicBuilder.name(TRANSACTION_TOPIC)
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic fraudDetectionTopic() {
        return TopicBuilder.name(FRAUD_DETECTION_TOPIC)
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic accountCountTopic() {
        return TopicBuilder.name(ACCOUNT_COUNT_TOPIC)
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic orderTopic() {
        return TopicBuilder.name(ORDER_TOPIC)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic orderDltTopic() {
        return TopicBuilder.name(ORDER_DLT_TOPIC)
                .partitions(3)
                .replicas(1)
                .build();
    }
}
