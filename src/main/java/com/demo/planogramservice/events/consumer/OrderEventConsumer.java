package com.demo.planogramservice.events.consumer;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.demo.planogramservice.events.config.KafkaTopicConfig;
import com.demo.planogramservice.events.domain.KafkaEventLogs;
import com.demo.planogramservice.events.domain.KafkaEventLogsRepository;
import com.demo.planogramservice.events.exception.OrderProcessingRetryableException;
import com.demo.shared.events.dto.OrderEvent;

/**
 * Consumer for OrderEvent messages from Kafka topic 'order-events'.
 * Demonstrates:
 * 1. Happy path event processing.
 * 2. Transient error handling & retry recovery without DLT.
 * 3. Retry exhaustion leading to DLT (Dead Letter Topic).
 * 4. Non-retryable / fatal errors bypassing retries directly to DLT.
 */
@Service
public class OrderEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderEventConsumer.class);
    public static final String GROUP_ID = "planogram-order-service-group";

    private final KafkaEventLogsRepository kafkaEventLogsRepository;

    // Track delivery attempts per order ID for scenario simulation
    private final Map<String, AtomicInteger> attemptTracker = new ConcurrentHashMap<>();

    // Metrics & in-memory audit
    private final AtomicInteger totalReceived = new AtomicInteger(0);
    private final AtomicInteger totalSuccess = new AtomicInteger(0);
    private final AtomicInteger totalRecovered = new AtomicInteger(0);
    private final AtomicInteger totalFailed = new AtomicInteger(0);
    private final AtomicReference<Map<String, Object>> lastOrder = new AtomicReference<>();
    private final List<Map<String, Object>> recentOrders = Collections.synchronizedList(new ArrayList<>());

    public OrderEventConsumer(KafkaEventLogsRepository kafkaEventLogsRepository) {
        this.kafkaEventLogsRepository = kafkaEventLogsRepository;
    }

    @KafkaListener(
            topics = KafkaTopicConfig.ORDER_TOPIC,
            groupId = GROUP_ID,
            containerFactory = "orderKafkaListenerContainerFactory"
    )
    public void consume(ConsumerRecord<String, Object> record) {
        int total = totalReceived.incrementAndGet();
        Object value = record.value();
        String orderId = null;
        String customerId = null;
        Double amount = null;
        String scenario = "NONE";
        String payloadJson = value != null ? value.toString() : "{}";

        if (value instanceof OrderEvent orderEvent) {
            orderId = orderEvent.getOrderId() != null ? orderEvent.getOrderId().toString() : record.key();
            customerId = orderEvent.getCustomerId() != null ? orderEvent.getCustomerId().toString() : "unknown";
            amount = orderEvent.getAmount();
            scenario = orderEvent.getFailureScenario() != null ? orderEvent.getFailureScenario().toString() : "NONE";
        } else if (value != null) {
            try {
                orderId = String.valueOf(value.getClass().getMethod("getOrderId").invoke(value));
                customerId = String.valueOf(value.getClass().getMethod("getCustomerId").invoke(value));
                amount = (Double) value.getClass().getMethod("getAmount").invoke(value);
                Object sc = value.getClass().getMethod("getFailureScenario").invoke(value);
                scenario = sc != null ? sc.toString() : "NONE";
            } catch (Exception e) {
                orderId = record.key() != null ? record.key() : "unknown";
            }
        }

        String trackingKey = (orderId != null) ? orderId : ("record-" + record.partition() + "-" + record.offset());
        int attempt = attemptTracker.computeIfAbsent(trackingKey, k -> new AtomicInteger(0)).incrementAndGet();

        log.info("[ORDER-CONSUMER] Processing order {} [attempt #{}, scenario={}, partition={}, offset={}, totalReceived={}]",
                orderId, attempt, scenario, record.partition(), record.offset(), total);

        // Execute scenario behavior
        switch (scenario.toUpperCase()) {
            case "TRANSIENT_RETRY" -> {
                if (attempt == 1) {
                    saveLog(record, orderId, "Simulated transient failure (attempt 1)", "RETRY_ATTEMPT_1",
                            "Simulated transient network timeout; retrying in 1s");
                    throw new OrderProcessingRetryableException(
                            "Simulated transient error on attempt 1 for order " + orderId);
                } else {
                    // Recovered on attempt 2!
                    totalSuccess.incrementAndGet();
                    totalRecovered.incrementAndGet();
                    recordSuccess(orderId, customerId, amount, scenario, record, "RECOVERED_AFTER_RETRY",
                            "Order recovered and processed successfully on attempt " + attempt);
                    attemptTracker.remove(trackingKey);
                }
            }

            case "EXHAUST_RETRY_TO_DLT" -> {
                totalFailed.incrementAndGet();
                saveLog(record, orderId, "Simulated persistent error (attempt " + attempt + ")",
                        "RETRY_ATTEMPT_" + attempt, "Persistent failure: payment gateway unreachable");
                throw new OrderProcessingRetryableException(
                        "Simulated persistent failure on attempt " + attempt + " for order " + orderId);
            }

            case "NON_RETRYABLE_FATAL" -> {
                totalFailed.incrementAndGet();
                saveLog(record, orderId, "Fatal non-retryable validation error", "FATAL_ERROR",
                        "Non-retryable IllegalArgumentException: routing directly to DLT");
                attemptTracker.remove(trackingKey);
                throw new IllegalArgumentException(
                        "Simulated fatal non-retryable error for order " + orderId + " - invalid order payload");
            }

            default -> {
                // NORMAL SUCCESS
                totalSuccess.incrementAndGet();
                recordSuccess(orderId, customerId, amount, scenario, record, "ORDER_SUCCESS",
                        "Order processed successfully on first attempt");
                attemptTracker.remove(trackingKey);
            }
        }
    }

    private void recordSuccess(
            String orderId,
            String customerId,
            Double amount,
            String scenario,
            ConsumerRecord<String, Object> record,
            String status,
            String message) {

        Map<String, Object> details = new HashMap<>();
        details.put("orderId", orderId);
        details.put("customerId", customerId);
        details.put("amount", amount);
        details.put("scenario", scenario);
        details.put("partition", record.partition());
        details.put("offset", record.offset());
        details.put("status", status);
        details.put("message", message);
        details.put("processedAt", Instant.now().toString());

        lastOrder.set(details);
        if (recentOrders.size() >= 25) {
            recentOrders.remove(0);
        }
        recentOrders.add(details);

        saveLog(record, orderId, message, status, null);
    }

    private void saveLog(
            ConsumerRecord<String, Object> record,
            String orderId,
            String message,
            String status,
            String errorMessage) {

        if (kafkaEventLogsRepository == null) return;
        try {
            KafkaEventLogs logEntry = KafkaEventLogs.builder()
                    .topic(record.topic())
                    .partitionId(record.partition())
                    .recordOffset(record.offset())
                    .eventKey(record.key() != null ? record.key().toString() : null)
                    .recordTimestamp(record.timestamp())
                    .timestampType(record.timestampType() != null ? record.timestampType().name() : null)
                    .consumerGroupId(GROUP_ID)
                    .workerName("OrderWorker")
                    .consumerClientId("planogram-order-worker")
                    .threadName(Thread.currentThread().getName())
                    .eventId(orderId)
                    .sourceService("planogram-order-service")
                    .message(message)
                    .consumedAt(Instant.now())
                    .status(status)
                    .errorMessage(errorMessage)
                    .payloadJson(record.value() != null ? record.value().toString() : null)
                    .build();

            kafkaEventLogsRepository.save(logEntry);
        } catch (Exception e) {
            log.warn("[ORDER-CONSUMER] Failed to persist order log entry: {}", e.getMessage());
        }
    }

    public Map<String, Object> getOrderStatus() {
        return Map.of(
                "consumerGroup", GROUP_ID,
                "topic", KafkaTopicConfig.ORDER_TOPIC,
                "totalReceived", totalReceived.get(),
                "totalSuccess", totalSuccess.get(),
                "totalRecoveredAfterRetry", totalRecovered.get(),
                "totalFailed", totalFailed.get(),
                "activeRetriesInProgress", attemptTracker.size(),
                "lastOrder", lastOrder.get() != null ? lastOrder.get() : Map.of("message", "No orders processed yet"),
                "recentOrders", new ArrayList<>(recentOrders)
        );
    }
}
