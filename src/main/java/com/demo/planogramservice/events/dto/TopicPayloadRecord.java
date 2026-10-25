package com.demo.planogramservice.events.dto;

/**
 * Record representing a single message payload fetched from a Kafka topic partition.
 */
public record TopicPayloadRecord(
        int partition,
        long offset,
        Object key,
        Object payload,
        String payloadType,
        long timestamp,
        String timestampIso
) {}
