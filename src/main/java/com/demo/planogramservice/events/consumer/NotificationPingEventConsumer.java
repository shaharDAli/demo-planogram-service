package com.demo.planogramservice.events.consumer;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
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
 * Consumer Group 3: notification-service-group
 * 
 * Demonstrates the Fan-Out (Publish-Subscribe) pattern in Apache Kafka.
 * Subscribes to the same topic 'demo-ping-topic' independently of
 * 'planogram-service-group' and 'audit-service-group'.
 * 
 * Whenever an event is published, all three consumer groups receive their
 * own copy with independent offsets.
 */
@Service
public class NotificationPingEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(NotificationPingEventConsumer.class);
    public static final String GROUP_ID = "notification-service-group";

    private final KafkaEventLogsRepository kafkaEventLogsRepository;

    public NotificationPingEventConsumer(KafkaEventLogsRepository kafkaEventLogsRepository) {
        this.kafkaEventLogsRepository = kafkaEventLogsRepository;
    }

    private final AtomicReference<PingEvent> lastReceivedPing = new AtomicReference<>();
    private final AtomicReference<Map<String, Object>> lastPingMap = new AtomicReference<>();
    private final AtomicInteger totalConsumed = new AtomicInteger(0);
    private final AtomicInteger lastPartition = new AtomicInteger(-1);
    private final AtomicLong lastOffset = new AtomicLong(-1);
    private final AtomicReference<String> lastReceivedAt = new AtomicReference<>();

    @KafkaListener(topics = KafkaTopicConfig.PING_TOPIC, groupId = GROUP_ID)
    public void consume(ConsumerRecord<String, Object> record, Acknowledgment acknowledgment) {
        Object value = record.value();
        int partition = record.partition();
        long offset = record.offset();
        String key = record.key();

        lastPartition.set(partition);
        lastOffset.set(offset);
        lastReceivedAt.set(Instant.now().toString());
        int count = totalConsumed.incrementAndGet();

        log.info("[NOTIFICATION-GROUP] Dispatching notification #{} for event from topic demo-ping-topic [partition={}, offset={}, key={}]",
                count, partition, offset, key);

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
            details.put("partition", partition);
            details.put("offset", offset);
            details.put("notifiedAt", Instant.now().toEpochMilli());
            lastPingMap.set(details);

            if (acknowledgment != null) {
                acknowledgment.acknowledge();
            }
        } else if (value != null) {
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
                details.put("partition", partition);
                details.put("offset", offset);
                details.put("notifiedAt", Instant.now().toEpochMilli());
                lastPingMap.set(details);

                if (acknowledgment != null) {
                    acknowledgment.acknowledge();
                }
            } catch (Exception e) {
                log.warn("[NOTIFICATION-GROUP] Could not extract ping event fields via reflection: {}", e.getMessage());
            }
        }

        saveLog(record, eventId, message, sourceService, payloadTimestamp);
    }

    private void saveLog(
            ConsumerRecord<String, Object> record,
            String eventId,
            String message,
            String sourceService,
            Long payloadTimestamp) {
        if (kafkaEventLogsRepository == null) return;
        try {
            KafkaEventLogs logEntry = KafkaEventLogs.builder()
                    .topic(record.topic())
                    .partitionId(record.partition())
                    .recordOffset(record.offset())
                    .eventKey(record.key() != null ? record.key().toString() : null)
                    .recordTimestamp(record.timestamp())
                    .timestampType(record.timestampType() != null ? record.timestampType().name() : null)
                    .leaderEpoch(record.leaderEpoch() != null && record.leaderEpoch().isPresent() ? record.leaderEpoch().get() : null)
                    .consumerGroupId(GROUP_ID)
                    .workerName("NotificationWorker")
                    .consumerClientId("notification-consumer-1")
                    .threadName(Thread.currentThread().getName())
                    .eventId(eventId)
                    .sourceService(sourceService)
                    .message(message)
                    .payloadTimestamp(payloadTimestamp)
                    .consumedAt(Instant.now())
                    .status("ACKNOWLEDGED")
                    .build();
            kafkaEventLogsRepository.save(logEntry);
        } catch (Exception e) {
            log.warn("[NOTIFICATION-GROUP] Failed to persist kafka event log: {}", e.getMessage());
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
        status.put("totalConsumed", totalConsumed.get());
        status.put("lastPartition", lastPartition.get());
        status.put("lastOffset", lastOffset.get());
        status.put("lastReceivedAt", lastReceivedAt.get());
        status.put("lastEvent", lastPingMap.get());
        return status;
    }
}
