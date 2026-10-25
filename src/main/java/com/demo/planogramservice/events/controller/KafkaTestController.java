package com.demo.planogramservice.events.controller;

import java.util.List;
import java.util.Map;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.demo.planogramservice.events.consumer.AuditPingEventConsumer;
import com.demo.planogramservice.events.consumer.FraudEventConsumer;
import com.demo.planogramservice.events.consumer.NotificationPingEventConsumer;
import com.demo.planogramservice.events.consumer.OrderDltConsumer;
import com.demo.planogramservice.events.consumer.OrderEventConsumer;
import com.demo.planogramservice.events.consumer.PingEventConsumer;
import com.demo.planogramservice.events.domain.KafkaEventLogsRepository;
import com.demo.planogramservice.events.dto.KafkaPullResultDto;
import com.demo.planogramservice.events.dto.TopicMessagesResponse;
import com.demo.planogramservice.events.service.KafkaEventPullService;
import com.demo.planogramservice.events.service.KafkaTopicPayloadService;
import com.demo.shared.events.dto.PingEvent;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;

@Tag(name = "Kafka Messaging & Audit Logs", description = "Kafka Consumer Group Telemetry, Event Logs, and Broker Record Sync APIs")
@RestController
@RequestMapping("/kafka")
public class KafkaTestController {

    private final PingEventConsumer pingEventConsumer;
    private final AuditPingEventConsumer auditPingEventConsumer;
    private final NotificationPingEventConsumer notificationPingEventConsumer;
    private final FraudEventConsumer fraudEventConsumer;
    private final OrderEventConsumer orderEventConsumer;
    private final OrderDltConsumer orderDltConsumer;
    private final KafkaEventLogsRepository kafkaEventLogsRepository;
    private final KafkaEventPullService kafkaEventPullService;
    private final KafkaTopicPayloadService kafkaTopicPayloadService;

    public KafkaTestController(
            PingEventConsumer pingEventConsumer,
            AuditPingEventConsumer auditPingEventConsumer,
            NotificationPingEventConsumer notificationPingEventConsumer,
            FraudEventConsumer fraudEventConsumer,
            OrderEventConsumer orderEventConsumer,
            OrderDltConsumer orderDltConsumer,
            KafkaEventLogsRepository kafkaEventLogsRepository,
            KafkaEventPullService kafkaEventPullService,
            KafkaTopicPayloadService kafkaTopicPayloadService) {
        this.pingEventConsumer = pingEventConsumer;
        this.auditPingEventConsumer = auditPingEventConsumer;
        this.notificationPingEventConsumer = notificationPingEventConsumer;
        this.fraudEventConsumer = fraudEventConsumer;
        this.orderEventConsumer = orderEventConsumer;
        this.orderDltConsumer = orderDltConsumer;
        this.kafkaEventLogsRepository = kafkaEventLogsRepository;
        this.kafkaEventPullService = kafkaEventPullService;
        this.kafkaTopicPayloadService = kafkaTopicPayloadService;
    }


    /**
     * Primary consumer group: planogram-service-group
     */
    @GetMapping("/ping/last")
    public ResponseEntity<?> getLastPing() {
        Map<String, Object> pingMap = pingEventConsumer.getLastPingMap();
        if (pingMap != null) {
            return ResponseEntity.ok(pingMap);
        }

        PingEvent lastPing = pingEventConsumer.getLastReceivedPing();
        if (lastPing == null) {
            return ResponseEntity.ok(Map.of("message", "No ping received yet by planogram-service-group"));
        }

        return ResponseEntity.ok(Map.of(
                "id", lastPing.getId().toString(),
                "message", lastPing.getMessage().toString(),
                "sourceService", lastPing.getSourceService().toString(),
                "timestamp", lastPing.getTimestamp()));
    }

    /**
     * Second consumer group: audit-service-group
     */
    @GetMapping("/audit/last")
    public ResponseEntity<?> getLastAuditPing() {
        Map<String, Object> pingMap = auditPingEventConsumer.getLastPingMap();
        if (pingMap != null) {
            return ResponseEntity.ok(pingMap);
        }
        return ResponseEntity.ok(Map.of("message", "No ping received yet by audit-service-group"));
    }

    /**
     * Third consumer group: notification-service-group
     */
    @GetMapping("/notification/last")
    public ResponseEntity<?> getLastNotificationPing() {
        Map<String, Object> pingMap = notificationPingEventConsumer.getLastPingMap();
        if (pingMap != null) {
            return ResponseEntity.ok(pingMap);
        }
        return ResponseEntity.ok(Map.of("message", "No ping received yet by notification-service-group"));
    }

    /**
     * Fan-Out status overview comparing all 3 independent consumer groups
     */
    @GetMapping("/consumer-groups")
    public ResponseEntity<?> getAllConsumerGroups() {
        return ResponseEntity.ok(Map.of(
                "topic", "demo-ping-topic",
                "pattern", "Fan-Out (Publish-Subscribe)",
                "consumerCount", 3,
                "groups", List.of(
                        pingEventConsumer.getGroupStatus(),
                        auditPingEventConsumer.getGroupStatus(),
                        notificationPingEventConsumer.getGroupStatus())));
    }

    /**
     * Inspect load-balancing across workers inside planogram-service-group
     */
    @GetMapping("/planogram/workers")
    public ResponseEntity<?> getPlanogramWorkers() {
        return ResponseEntity.ok(pingEventConsumer.getGroupStatus());
    }

    /**
     * Complete historical audit trail of all Kafka event coordinates logged in
     * MySQL
     */
    @GetMapping("/logs")
    public ResponseEntity<?> getKafkaLogs() {
        return ResponseEntity.ok(kafkaEventLogsRepository.findTop50ByOrderByConsumedAtDesc());
    }

    /**
     * Pulls existing events from Kafka topic across all partitions starting from offset 0
     * and saves any unlogged events into the kafka_event_logs table.
     */
    @PostMapping("/pull-existing")
    public ResponseEntity<KafkaPullResultDto> pullExistingEvents(
            @RequestParam(name = "topic", defaultValue = "demo-ping-topic") String topic) {
        return ResponseEntity.ok(kafkaEventPullService.pullExistingEvents(topic));
    }

    @GetMapping("/pull-existing")
    public ResponseEntity<KafkaPullResultDto> pullExistingEventsGet(
            @RequestParam(name = "topic", defaultValue = "demo-ping-topic") String topic) {
        return ResponseEntity.ok(kafkaEventPullService.pullExistingEvents(topic));
    }

    /**
     * Inspect Kafka Streams fraud detection telemetries and detected fraudulent transactions
     */
    @GetMapping("/fraud/status")
    public ResponseEntity<?> getFraudStatus() {
        return ResponseEntity.ok(fraudEventConsumer.getFraudStatus());
    }

    /**
     * Fetch all payloads added in a Kafka topic with request param 'topic'.
     * Dynamically deserializes Avro (Schema Registry), Long (Kafka Streams count), JSON, or Strings.
     */
    @Operation(summary = "Fetch all payloads from a Kafka topic", description = "Reads all messages starting from offset 0 across all partitions for the given topic and returns their decoded payloads.")
    @GetMapping(value = {"/payloads", "/topic/payloads"})
    public ResponseEntity<TopicMessagesResponse> getTopicPayloads(
            @Parameter(description = "Kafka topic name (e.g. account-count-topic, transaction-events, demo-ping-topic)", required = true)
            @RequestParam(name = "topic") String topic,
            @Parameter(description = "Maximum number of messages to return (default 500, max 5000)")
            @RequestParam(name = "limit", required = false, defaultValue = "500") Integer limit) {
        return ResponseEntity.ok(kafkaTopicPayloadService.fetchTopicPayloads(topic, limit));
    }

    /**
     * Order Event Consumer telemetry, processing metrics, and retry recovery history
     */
    @Operation(summary = "Inspect Order Event Consumer telemetry and retry metrics")
    @GetMapping("/order/status")
    public ResponseEntity<?> getOrderStatus() {
        return ResponseEntity.ok(orderEventConsumer.getOrderStatus());
    }

    /**
     * Inspect Dead Letter Topic (DLT) received events and exception headers
     */
    @Operation(summary = "Inspect Dead Letter Topic (order-events.DLT) telemetry and failed events")
    @GetMapping("/order/dlt")
    public ResponseEntity<?> getOrderDltStatus() {
        return ResponseEntity.ok(orderDltConsumer.getDltStatus());
    }
}

