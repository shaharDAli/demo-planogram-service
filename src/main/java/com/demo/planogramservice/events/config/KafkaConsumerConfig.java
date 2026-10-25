package com.demo.planogramservice.events.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import io.confluent.kafka.serializers.KafkaAvroSerializer;

/**
 * Kafka Consumer and Error Handling Configuration.
 *
 * Configures Spring Kafka's DefaultErrorHandler:
 * 1. DeadLetterPublishingRecoverer: Automatically forwards failed records to the DLT topic (<topic>.DLT).
 * 2. BackOff Strategy: FixedBackOff(1000ms delay, 2 retries = 3 total delivery attempts).
 * 3. Classification:
 *    - Retryable Exceptions: Retried according to the BackOff policy.
 *    - Non-Retryable Exceptions (e.g. IllegalArgumentException, SerializationException):
 *      Sent directly to DLT without retrying.
 * 4. Retry Listener: Logs delivery attempt details for observability and debugging.
 */
@Configuration
public class KafkaConsumerConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerConfig.class);

    public static final String BOOTSTRAP_SERVERS = "localhost:9092";
    public static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";
    public static final long RETRY_INTERVAL_MS = 1000L;
    public static final long MAX_RETRY_ATTEMPTS = 2L; // 1 initial delivery + 2 retries = 3 total attempts

    /**
     * Dedicated producer factory strictly for Dead Letter Topic (DLT) routing.
     * Keeps application.properties free of producer properties in a consumer service.
     */
    @Bean
    public ProducerFactory<String, Object> dltProducerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put("schema.registry.url", SCHEMA_REGISTRY_URL);
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, Object> dltKafkaTemplate() {
        return new KafkaTemplate<>(dltProducerFactory());
    }

    @Bean
    public DefaultErrorHandler defaultErrorHandler(@Qualifier("dltKafkaTemplate") KafkaTemplate<String, Object> dltKafkaTemplate) {
        // 1. Recoverer: publishes exhausted or non-retryable records to Dead Letter Topic (<originalTopic>.DLT)
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                dltKafkaTemplate,
                (record, exception) -> {
                    String dltTopic = record.topic() + ".DLT";
                    log.error("[DEFAULT-ERROR-HANDLER] Routing failed record [key={}, topic={}, partition={}, offset={}] to DLT topic '{}'. Reason: {}",
                            record.key(), record.topic(), record.partition(), record.offset(), dltTopic, exception.getMessage());
                    return new TopicPartition(dltTopic, record.partition());
                }
        );

        // 2. FixedBackOff: 1000ms delay between retries, up to 2 retries (total 3 attempts)
        FixedBackOff backOff = new FixedBackOff(RETRY_INTERVAL_MS, MAX_RETRY_ATTEMPTS);

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff);

        // 3. Configure Non-Retryable / Fatal exceptions
        errorHandler.addNotRetryableExceptions(
                IllegalArgumentException.class,
                IllegalStateException.class,
                SerializationException.class
        );

        // 4. Retry Listener for audit logging on each retry attempt
        errorHandler.setRetryListeners((record, exception, deliveryAttempt) -> {
            log.warn("[DEFAULT-ERROR-HANDLER] Retry attempt #{} for topic: {}, partition: {}, offset: {}, key: {}. Exception: {}",
                    deliveryAttempt, record.topic(), record.partition(), record.offset(), record.key(), exception.getMessage());
        });

        return errorHandler;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> orderKafkaListenerContainerFactory(
            ConsumerFactory<String, Object> consumerFactory,
            DefaultErrorHandler defaultErrorHandler) {

        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(defaultErrorHandler);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        return factory;
    }
}
