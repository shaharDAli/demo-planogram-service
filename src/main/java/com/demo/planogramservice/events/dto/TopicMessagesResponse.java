package com.demo.planogramservice.events.dto;

import java.util.List;

/**
 * Response DTO returned when fetching payloads for a given Kafka topic.
 */
public record TopicMessagesResponse(
        String status,
        String topic,
        int partitionCount,
        long totalMessagesOnBroker,
        int returnedCount,
        List<TopicPayloadRecord> messages,
        String message
) {}
