package com.demo.planogramservice.events.consumer;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import com.demo.planogramservice.events.config.KafkaTopicConfig;
import com.demo.planogramservice.events.domain.KafkaEventLogs;
import com.demo.planogramservice.events.domain.KafkaEventLogsRepository;
import com.demo.shared.events.dto.PingEvent;

/**
 * Consumer Group: planogram-service-group (Load Balancing / Competing Consumers
 * Pattern)
 * 
 * Demonstrates MULTIPLE CONSUMERS INSIDE A SINGLE CONSUMER GROUP:
 * 
 * All workers share the same 'groupId' ("planogram-service-group").
 * In Apache Kafka, when multiple consumers share the same groupId:
 * 1. They DO NOT duplicate messages. Instead, they divide the topic's
 * partitions among themselves.
 * 2. Topic 'demo-ping-topic' has 5 partitions (0, 1, 2, 3, 4).
 * 3. Kafka assigns a subset of partitions to Worker-1, Worker-2, and Worker-3.
 * 4. Each published event is processed by EXACTLY ONE worker inside this group
 * (load-balanced).
 */
@Service
public class PingEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(PingEventConsumer.class);
    public static final String GROUP_ID = "planogram-service-group";

    private final KafkaEventLogsRepository kafkaEventLogsRepository;

    public PingEventConsumer(KafkaEventLogsRepository kafkaEventLogsRepository) {
        this.kafkaEventLogsRepository = kafkaEventLogsRepository;
    }

    private final AtomicReference<PingEvent> lastReceivedPing = new AtomicReference<>();
    private final AtomicReference<Map<String, Object>> lastPingMap = new AtomicReference<>();

    // Group-level counters
    private final AtomicInteger totalConsumed = new AtomicInteger(0);
    private final AtomicInteger lastPartition = new AtomicInteger(-1);
    private final AtomicLong lastOffset = new AtomicLong(-1);
    private final AtomicReference<String> lastReceivedAt = new AtomicReference<>();
    private final AtomicReference<String> lastWorker = new AtomicReference<>("None");

    // Per-worker counters
    private final AtomicInteger worker1Count = new AtomicInteger(0);
    private final AtomicInteger worker2Count = new AtomicInteger(0);
    private final AtomicInteger worker3Count = new AtomicInteger(0);

    // Track which partitions each worker has processed
    private final Set<Integer> worker1Partitions = ConcurrentHashMap.newKeySet();
    private final Set<Integer> worker2Partitions = ConcurrentHashMap.newKeySet();
    private final Set<Integer> worker3Partitions = ConcurrentHashMap.newKeySet();

    /**
     * Consumer Instance 1 (Worker 1 in planogram-service-group)
     */
    @KafkaListener(id = "planogram-consumer-worker-1", topics = KafkaTopicConfig.PING_TOPIC, groupId = GROUP_ID, clientIdPrefix = "planogram-worker-1")
    public void consumeWorker1(ConsumerRecord<String, Object> record, Acknowledgment acknowledgment) {
        worker1Count.incrementAndGet();
        worker1Partitions.add(record.partition());
        processRecord("Worker-1", record, acknowledgment);
    }

    /**
     * Consumer Instance 2 (Worker 2 in planogram-service-group)
     */
    @KafkaListener(id = "planogram-consumer-worker-2", topics = KafkaTopicConfig.PING_TOPIC, groupId = GROUP_ID, clientIdPrefix = "planogram-worker-2")
    public void consumeWorker2(ConsumerRecord<String, Object> record, Acknowledgment acknowledgment) {
        worker2Count.incrementAndGet();
        worker2Partitions.add(record.partition());
        processRecord("Worker-2", record, acknowledgment);
    }

    /**
     * Consumer Instance 3 (Worker 3 in planogram-service-group)
     */
    @KafkaListener(id = "planogram-consumer-worker-3", topics = KafkaTopicConfig.PING_TOPIC, groupId = GROUP_ID, clientIdPrefix = "planogram-worker-3")
    public void consumeWorker3(ConsumerRecord<String, Object> record, Acknowledgment acknowledgment) {
        worker3Count.incrementAndGet();
        worker3Partitions.add(record.partition());
        processRecord("Worker-3", record, acknowledgment);
    }

    /**
     * Common processing logic for all workers in this consumer group
     */
    private void processRecord(String workerName, ConsumerRecord<String, Object> record,
            Acknowledgment acknowledgment) {
        Object value = record.value();
        int partition = record.partition();
        long offset = record.offset();
        String key = record.key();

        lastPartition.set(partition);
        lastOffset.set(offset);
        lastReceivedAt.set(Instant.now().toString());
        lastWorker.set(workerName);
        int total = totalConsumed.incrementAndGet();

        log.info("[PLANOGRAM-GROUP] [{}] Received record #{} (key={}) -> [partition={}, offset={}, thread={}]",
                workerName, total, key, partition, offset, Thread.currentThread().getName());

        String eventId = null;
        String message = null;
        String sourceService = null;
        Long payloadTimestamp = null;

        if (value instanceof PingEvent pingEvent) {
            lastReceivedPing.set(pingEvent);
            eventId = pingEvent.getId().toString();
            message = pingEvent.getMessage().toString();
            sourceService = pingEvent.getSourceService().toString();
            payloadTimestamp = pingEvent.getTimestamp() != null ? pingEvent.getTimestamp().toEpochMilli() : null;

            Map<String, Object> details = new HashMap<>();
            details.put("id", eventId);
            details.put("message", message);
            details.put("sourceService", sourceService);
            details.put("timestamp", payloadTimestamp);
            details.put("groupId", GROUP_ID);
            details.put("worker", workerName);
            details.put("partition", partition);
            details.put("offset", offset);
            details.put("thread", Thread.currentThread().getName());
            lastPingMap.set(details);

            if (acknowledgment != null) {
                acknowledgment.acknowledge();
            }
        } else if (value != null) {
            // Safe extraction if value was loaded by a different ClassLoader
            try {
                eventId = String.valueOf(value.getClass().getMethod("getId").invoke(value));
                message = String.valueOf(value.getClass().getMethod("getMessage").invoke(value));
                sourceService = String.valueOf(value.getClass().getMethod("getSourceService").invoke(value));
                Object ts = value.getClass().getMethod("getTimestamp").invoke(value);
                payloadTimestamp = ts instanceof Number num ? num.longValue() : System.currentTimeMillis();

                Map<String, Object> details = new HashMap<>();
                details.put("id", eventId);
                details.put("message", message);
                details.put("sourceService", sourceService);
                details.put("timestamp", payloadTimestamp);
                details.put("groupId", GROUP_ID);
                details.put("worker", workerName);
                details.put("partition", partition);
                details.put("offset", offset);
                details.put("thread", Thread.currentThread().getName());
                lastPingMap.set(details);

                if (acknowledgment != null) {
                    acknowledgment.acknowledge();
                }
            } catch (Exception e) {
                log.warn("[PLANOGRAM-GROUP] [{}] Could not extract ping event fields via reflection: {}", workerName,
                        e.getMessage());
            }
        }

        saveLog(workerName, record, eventId, message, sourceService, payloadTimestamp, "ACKNOWLEDGED", null);
    }

    private void saveLog(
            String workerName,
            ConsumerRecord<String, Object> record,
            String eventId,
            String message,
            String sourceService,
            Long payloadTimestamp,
            String status,
            String errorMessage) {
        if (kafkaEventLogsRepository == null)
            return;
        try {
            KafkaEventLogs logEntry = KafkaEventLogs.builder()
                    .topic(record.topic())
                    .partitionId(record.partition())
                    .recordOffset(record.offset())
                    .eventKey(record.key() != null ? record.key().toString() : null)
                    .recordTimestamp(record.timestamp())
                    .timestampType(record.timestampType() != null ? record.timestampType().name() : null)
                    .leaderEpoch(record.leaderEpoch() != null && record.leaderEpoch().isPresent()
                            ? record.leaderEpoch().get()
                            : null)
                    .consumerGroupId(GROUP_ID)
                    .workerName(workerName)
                    .consumerClientId("planogram-" + workerName.toLowerCase())
                    .threadName(Thread.currentThread().getName())
                    .eventId(eventId)
                    .sourceService(sourceService)
                    .message(message)
                    .payloadTimestamp(payloadTimestamp)
                    .consumedAt(Instant.now())
                    .status(status)
                    .errorMessage(errorMessage)
                    .build();
            kafkaEventLogsRepository.save(logEntry);
        } catch (Exception e) {
            log.warn("[PLANOGRAM-GROUP] [{}] Failed to persist kafka event log: {}", workerName, e.getMessage());
        }
    }

    public PingEvent getLastReceivedPing() {
        return lastReceivedPing.get();
    }

    public Map<String, Object> getLastPingMap() {
        return lastPingMap.get();
    }

    public int getTotalConsumed() {
        return totalConsumed.get();
    }

    public Map<String, Object> getGroupStatus() {
        Map<String, Object> status = new HashMap<>();
        status.put("groupId", GROUP_ID);
        status.put("consumerClass", getClass().getSimpleName());
        status.put("pattern", "Load Balancing / Competing Consumers");
        status.put("totalConsumed", totalConsumed.get());
        status.put("lastWorker", lastWorker.get());
        status.put("lastPartition", lastPartition.get());
        status.put("lastOffset", lastOffset.get());
        status.put("lastReceivedAt", lastReceivedAt.get());
        status.put("lastEvent", lastPingMap.get());
        status.put("workers", Map.of(
                "Worker-1", Map.of("processedCount", worker1Count.get(), "partitions", worker1Partitions),
                "Worker-2", Map.of("processedCount", worker2Count.get(), "partitions", worker2Partitions),
                "Worker-3", Map.of("processedCount", worker3Count.get(), "partitions", worker3Partitions)));
        return status;
    }
}
