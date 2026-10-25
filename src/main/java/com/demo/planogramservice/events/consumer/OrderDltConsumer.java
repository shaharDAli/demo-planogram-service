package com.demo.planogramservice.events.consumer;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Service;

import com.demo.planogramservice.events.config.KafkaTopicConfig;
import com.demo.planogramservice.events.domain.KafkaEventLogs;
import com.demo.planogramservice.events.domain.KafkaEventLogsRepository;
import com.demo.shared.events.dto.OrderEvent;

/**
 * Consumer for Dead Letter Topic 'order-events.DLT'.
 * Consumes events that failed retries in the main consumer or triggered non-retryable exceptions.
 * Inspects standard Spring Kafka DLT headers:
 * - kafka_dlt-original-topic
 * - kafka_dlt-original-partition
 * - kafka_dlt-original-offset
 * - kafka_dlt-exception-message
 * - kafka_dlt-exception-fqcn
 */
@Service
public class OrderDltConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderDltConsumer.class);
    public static final String GROUP_ID = "planogram-order-dlt-group";

    private final KafkaEventLogsRepository kafkaEventLogsRepository;

    private final AtomicInteger totalDltReceived = new AtomicInteger(0);
    private final AtomicReference<Map<String, Object>> lastDltEvent = new AtomicReference<>();
    private final List<Map<String, Object>> recentDltEvents = Collections.synchronizedList(new ArrayList<>());

    public OrderDltConsumer(KafkaEventLogsRepository kafkaEventLogsRepository) {
        this.kafkaEventLogsRepository = kafkaEventLogsRepository;
    }

    @KafkaListener(topics = KafkaTopicConfig.ORDER_DLT_TOPIC, groupId = GROUP_ID)
    public void consumeDlt(ConsumerRecord<String, Object> record) {
        int count = totalDltReceived.incrementAndGet();
        Object value = record.value();

        String orderId = null;
        if (value instanceof OrderEvent orderEvent) {
            orderId = orderEvent.getOrderId() != null ? orderEvent.getOrderId().toString() : record.key();
        } else if (value != null) {
            try {
                orderId = String.valueOf(value.getClass().getMethod("getOrderId").invoke(value));
            } catch (Exception e) {
                orderId = record.key() != null ? record.key() : "unknown";
            }
        }

        // Extract standard Spring Kafka DLT headers
        String origTopic = getHeaderString(record, KafkaHeaders.DLT_ORIGINAL_TOPIC);
        String origPartition = getHeaderString(record, KafkaHeaders.DLT_ORIGINAL_PARTITION);
        String origOffset = getHeaderString(record, KafkaHeaders.DLT_ORIGINAL_OFFSET);
        String exMessage = getHeaderString(record, KafkaHeaders.DLT_EXCEPTION_MESSAGE);
        String exClass = getHeaderString(record, KafkaHeaders.DLT_EXCEPTION_FQCN);

        log.error("[ORDER-DLT-CONSUMER] Captured DLT event #{} for order {} from topic {} [origTopic={}, origPartition={}, origOffset={}, exMessage={}]",
                count, orderId, record.topic(), origTopic, origPartition, origOffset, exMessage);

        Map<String, Object> dltDetails = new HashMap<>();
        dltDetails.put("orderId", orderId);
        dltDetails.put("dltTopic", record.topic());
        dltDetails.put("dltPartition", record.partition());
        dltDetails.put("dltOffset", record.offset());
        dltDetails.put("originalTopic", origTopic);
        dltDetails.put("originalPartition", origPartition);
        dltDetails.put("originalOffset", origOffset);
        dltDetails.put("exceptionMessage", exMessage);
        dltDetails.put("exceptionClass", exClass);
        dltDetails.put("receivedAt", Instant.now().toString());
        dltDetails.put("payload", value != null ? value.toString() : "{}");

        lastDltEvent.set(dltDetails);
        if (recentDltEvents.size() >= 25) {
            recentDltEvents.remove(0);
        }
        recentDltEvents.add(dltDetails);

        if (kafkaEventLogsRepository != null) {
            try {
                KafkaEventLogs logEntry = KafkaEventLogs.builder()
                        .topic(record.topic())
                        .partitionId(record.partition())
                        .recordOffset(record.offset())
                        .eventKey(record.key() != null ? record.key().toString() : null)
                        .recordTimestamp(record.timestamp())
                        .timestampType(record.timestampType() != null ? record.timestampType().name() : null)
                        .consumerGroupId(GROUP_ID)
                        .workerName("OrderDltWorker")
                        .consumerClientId("planogram-order-dlt-worker")
                        .threadName(Thread.currentThread().getName())
                        .eventId(orderId)
                        .sourceService("planogram-order-dlt")
                        .message("Message moved to Dead Letter Topic. Original: " + origTopic + " [p=" + origPartition + ", o=" + origOffset + "]")
                        .consumedAt(Instant.now())
                        .status("DLT_RECEIVED")
                        .errorMessage(exMessage != null ? exMessage : "Exhausted retries or fatal exception")
                        .payloadJson(value != null ? value.toString() : null)
                        .headersJson(String.format("{\"origTopic\":\"%s\",\"origPartition\":\"%s\",\"origOffset\":\"%s\",\"exceptionClass\":\"%s\"}",
                                origTopic, origPartition, origOffset, exClass))
                        .build();

                kafkaEventLogsRepository.save(logEntry);
            } catch (Exception e) {
                log.warn("[ORDER-DLT-CONSUMER] Failed to persist DLT event log: {}", e.getMessage());
            }
        }
    }

    private String getHeaderString(ConsumerRecord<?, ?> record, String headerKey) {
        Header header = record.headers().lastHeader(headerKey);
        if (header != null && header.value() != null) {
            return new String(header.value(), StandardCharsets.UTF_8);
        }
        return "N/A";
    }

    public Map<String, Object> getDltStatus() {
        return Map.of(
                "consumerGroup", GROUP_ID,
                "dltTopic", KafkaTopicConfig.ORDER_DLT_TOPIC,
                "totalDltReceived", totalDltReceived.get(),
                "lastDltEvent", lastDltEvent.get() != null ? lastDltEvent.get() : Map.of("message", "No DLT events received yet"),
                "recentDltEvents", new ArrayList<>(recentDltEvents)
        );
    }
}
