package com.demo.planogramservice.events.service;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.demo.planogramservice.events.dto.TopicMessagesResponse;
import com.demo.planogramservice.events.dto.TopicPayloadRecord;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

/**
 * Service to inspect and fetch all message payloads from any Kafka topic.
 * Uses a safe byte-array consumer combined with dynamic deserialization to seamlessly
 * support Avro (Schema Registry), 8-byte Long (Streams count/aggregates), JSON, and Plain Text.
 */
@Service
public class KafkaTopicPayloadService {

    private static final Logger log = LoggerFactory.getLogger(KafkaTopicPayloadService.class);

    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.properties.schema.registry.url:http://localhost:8081}")
    private String schemaRegistryUrl;

    private final ObjectMapper objectMapper;
    private KafkaAvroDeserializer avroDeserializer;

    public KafkaTopicPayloadService(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @PostConstruct
    public void init() {
        try {
            avroDeserializer = new KafkaAvroDeserializer();
            Map<String, Object> config = new HashMap<>();
            config.put("schema.registry.url", schemaRegistryUrl);
            config.put("specific.avro.reader", false);
            avroDeserializer.configure(config, false);
            log.info("[PAYLOAD-SERVICE] KafkaAvroDeserializer initialized with Schema Registry at: {}", schemaRegistryUrl);
        } catch (Exception e) {
            log.warn("[PAYLOAD-SERVICE] Failed to initialize KafkaAvroDeserializer with Schema Registry: {}", e.getMessage());
        }
    }

    @PreDestroy
    public void destroy() {
        if (avroDeserializer != null) {
            try {
                avroDeserializer.close();
            } catch (Exception ignored) {
            }
        }
    }

    /**
     * Fetches all message payloads from the specified topic from offset 0 up to end offsets.
     *
     * @param topic the Kafka topic name (e.g. "account-count-topic", "transaction-events")
     * @param limit maximum number of records to return (defaults to 500)
     * @return TopicMessagesResponse containing the list of payloads and metadata
     */
    public TopicMessagesResponse fetchTopicPayloads(String topic, Integer limit) {
        if (topic == null || topic.isBlank()) {
            return new TopicMessagesResponse(
                    "INVALID_REQUEST",
                    topic,
                    0,
                    0L,
                    0,
                    List.of(),
                    "Request parameter 'topic' is required and cannot be blank."
            );
        }

        int maxRecords = (limit == null || limit <= 0) ? 500 : Math.min(limit, 5000);

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "topic-payload-fetcher-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 5000);
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);

        KafkaConsumer<byte[], byte[]> consumer = null;

        try {
            consumer = new KafkaConsumer<>(props);

            List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic, Duration.ofSeconds(4));
            if (partitionInfos == null || partitionInfos.isEmpty()) {
                log.warn("[PAYLOAD-SERVICE] Topic '{}' not found or has no partitions.", topic);
                return new TopicMessagesResponse(
                        "NOT_FOUND",
                        topic,
                        0,
                        0L,
                        0,
                        List.of(),
                        "Topic '" + topic + "' does not exist or has no active partitions on Kafka broker."
                );
            }

            List<TopicPartition> partitions = partitionInfos.stream()
                    .map(p -> new TopicPartition(p.topic(), p.partition()))
                    .sorted(Comparator.comparingInt(TopicPartition::partition))
                    .toList();

            consumer.assign(partitions);

            Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(partitions, Duration.ofSeconds(3));
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions, Duration.ofSeconds(3));

            long totalMessages = 0;
            List<TopicPartition> activePartitions = new ArrayList<>();

            for (TopicPartition tp : partitions) {
                long beg = beginningOffsets.getOrDefault(tp, 0L);
                long end = endOffsets.getOrDefault(tp, 0L);
                long diff = Math.max(0, end - beg);
                totalMessages += diff;
                if (end > beg) {
                    activePartitions.add(tp);
                }
            }

            if (totalMessages == 0) {
                return new TopicMessagesResponse(
                        "SUCCESS",
                        topic,
                        partitions.size(),
                        0L,
                        0,
                        List.of(),
                        "Topic '" + topic + "' is currently empty (0 messages on broker)."
                );
            }

            // Seek all partitions to earliest offset (0)
            consumer.seekToBeginning(partitions);

            List<TopicPayloadRecord> messages = new ArrayList<>();
            int emptyStreak = 0;
            int maxEmptyStreak = 3;
            int iteration = 0;
            int maxIterations = 50;

            while (messages.size() < maxRecords && emptyStreak < maxEmptyStreak && iteration++ < maxIterations) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(300));

                if (records.isEmpty()) {
                    emptyStreak++;
                } else {
                    emptyStreak = 0;
                    for (ConsumerRecord<byte[], byte[]> record : records) {
                        Object key = deserializeKey(record.key());
                        Object payload = deserializeValue(record.topic(), record.value());
                        String payloadType = determinePayloadType(record.value(), payload);

                        messages.add(new TopicPayloadRecord(
                                record.partition(),
                                record.offset(),
                                key,
                                payload,
                                payloadType,
                                record.timestamp(),
                                record.timestamp() > 0 ? Instant.ofEpochMilli(record.timestamp()).toString() : null
                        ));

                        if (messages.size() >= maxRecords) {
                            break;
                        }
                    }
                }

                // Check if all partitions with data have caught up to endOffsets
                boolean allCaughtUp = true;
                for (TopicPartition tp : activePartitions) {
                    long targetEnd = endOffsets.getOrDefault(tp, 0L);
                    long currentPos = consumer.position(tp);
                    if (currentPos < targetEnd) {
                        allCaughtUp = false;
                        break;
                    }
                }
                if (allCaughtUp) {
                    break;
                }
            }

            // Sort deterministically by partition and offset
            messages.sort(Comparator.comparingInt(TopicPayloadRecord::partition)
                    .thenComparingLong(TopicPayloadRecord::offset));

            return new TopicMessagesResponse(
                    "SUCCESS",
                    topic,
                    partitions.size(),
                    totalMessages,
                    messages.size(),
                    messages,
                    String.format("Successfully retrieved %d message payload(s) from topic '%s' (total available on broker: %d).",
                            messages.size(), topic, totalMessages)
            );

        } catch (Exception e) {
            log.error("[PAYLOAD-SERVICE] Error fetching payloads for topic '{}': {}", topic, e.getMessage(), e);
            return new TopicMessagesResponse(
                    "ERROR",
                    topic,
                    0,
                    0L,
                    0,
                    List.of(),
                    "Failed to fetch payloads from topic '" + topic + "': " + e.getMessage()
            );
        } finally {
            if (consumer != null) {
                try {
                    consumer.close(Duration.ofSeconds(1));
                } catch (Exception ignored) {
                }
            }
        }
    }

    private Object deserializeKey(byte[] keyBytes) {
        if (keyBytes == null) {
            return null;
        }
        return new String(keyBytes, StandardCharsets.UTF_8);
    }

    private Object deserializeValue(String topic, byte[] valueBytes) {
        if (valueBytes == null) {
            return null;
        }
        if (valueBytes.length == 0) {
            return "";
        }

        // 1. Confluent Schema Registry Avro format (starts with Magic Byte 0x00 and 4-byte schema ID)
        if (valueBytes.length > 5 && valueBytes[0] == 0x00) {
            try {
                if (avroDeserializer != null) {
                    Object avroObj = avroDeserializer.deserialize(topic, valueBytes);
                    if (avroObj != null) {
                        try {
                            return objectMapper.readTree(avroObj.toString());
                        } catch (Exception ignored) {
                            return avroObj.toString();
                        }
                    }
                }
            } catch (Exception e) {
                log.debug("[PAYLOAD-SERVICE] Avro deserialization skipped for topic '{}': {}", topic, e.getMessage());
            }
        }

        // 2. 8-byte Long (e.g., Kafka Streams count() Serdes.Long())
        if (valueBytes.length == 8) {
            boolean startsWithZeros = (valueBytes[0] == 0 && valueBytes[1] == 0 && valueBytes[2] == 0);
            boolean isPrintable = isPrintableString(valueBytes);
            if (startsWithZeros || !isPrintable) {
                return ByteBuffer.wrap(valueBytes).getLong();
            }
        }

        // 3. 4-byte Integer (e.g., Serdes.Integer())
        if (valueBytes.length == 4) {
            boolean startsWithZeros = (valueBytes[0] == 0 && valueBytes[1] == 0);
            boolean isPrintable = isPrintableString(valueBytes);
            if (startsWithZeros || !isPrintable) {
                return ByteBuffer.wrap(valueBytes).getInt();
            }
        }

        // 4. UTF-8 String or JSON
        try {
            String str = new String(valueBytes, StandardCharsets.UTF_8);
            String trimmed = str.trim();
            if ((trimmed.startsWith("{") && trimmed.endsWith("}")) ||
                (trimmed.startsWith("[") && trimmed.endsWith("]"))) {
                try {
                    return objectMapper.readTree(trimmed);
                } catch (Exception ignored) {
                    return str;
                }
            }
            return str;
        } catch (Exception e) {
            // 5. Fallback to Hex format
            return HexFormat.of().formatHex(valueBytes);
        }
    }

    private String determinePayloadType(byte[] bytes, Object parsed) {
        if (bytes == null) {
            return "NULL";
        }
        if (bytes.length > 5 && bytes[0] == 0x00) {
            return "AVRO";
        }
        if (parsed instanceof Long) {
            return "LONG";
        }
        if (parsed instanceof Integer) {
            return "INTEGER";
        }
        if (parsed instanceof JsonNode) {
            return "JSON";
        }
        if (parsed instanceof String) {
            return "STRING";
        }
        return "BYTES";
    }

    private boolean isPrintableString(byte[] bytes) {
        for (byte b : bytes) {
            if ((b < 32 && b != 9 && b != 10 && b != 13) || b > 126) {
                return false;
            }
        }
        return true;
    }
}
