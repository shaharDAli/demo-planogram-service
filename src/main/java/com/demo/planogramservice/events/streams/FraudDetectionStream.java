package com.demo.planogramservice.events.streams;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

import com.demo.planogramservice.events.config.KafkaTopicConfig;
import com.demo.shared.events.dto.Transaction;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.extern.slf4j.Slf4j;

@EnableKafkaStreams
@Configuration
@Slf4j
public class FraudDetectionStream {

    public static final String APPLICATION_ID = "demo-planogram-fraud-detection-streams";
    public static final String BOOTSTRAP_SERVERS = "localhost:9092";
    public static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";
    public static final double FRAUD_AMOUNT_THRESHOLD = 1000.0;
    public static final String ACCOUNT_COUNT_STORE = "account-count-store";

    /**
     * In-code Kafka Streams configuration using constants to avoid polluting application.properties.
     */
    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kStreamsConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class.getName());
        props.put("schema.registry.url", SCHEMA_REGISTRY_URL);
        props.put("specific.avro.reader", "true");
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                LogAndContinueExceptionHandler.class.getName());
        return new KafkaStreamsConfiguration(props);
    }

    @Autowired
    public void configureStreamsBuilderFactoryBean(StreamsBuilderFactoryBean factoryBean) {
        factoryBean.setStreamsUncaughtExceptionHandler(exception -> {
            log.error("Uncaught exception in Kafka Streams thread: {}", exception.getMessage(), exception);
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
        });
    }

    @Bean
    public KStream<String, Transaction> fraudDetectionTopology(StreamsBuilder streamsBuilder) {

        KStream<String, Transaction> stream = streamsBuilder.stream(KafkaTopicConfig.TRANSACTION_TOPIC);

        KTable<String, Long> accountCountTable = stream
                .filter((key, transaction) -> transaction != null && transaction.getAccountId() != null)
                .selectKey((key, transaction) -> transaction.getAccountId().toString())
                .groupByKey()
                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as(ACCOUNT_COUNT_STORE)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long()));

        accountCountTable.toStream().to(KafkaTopicConfig.ACCOUNT_COUNT_TOPIC,
                Produced.with(Serdes.String(), Serdes.Long()));

        KStream<String, Transaction> fraudStream = stream
                .filter((key, transaction) -> transaction != null && transaction.getAmount() > FRAUD_AMOUNT_THRESHOLD)
                .peek((key, transaction) -> log.warn("Suspicious transaction detected: key={}, amount={}", key,
                        transaction.getAmount()));
        fraudStream.to(KafkaTopicConfig.FRAUD_DETECTION_TOPIC);

        return fraudStream;
    }
}
