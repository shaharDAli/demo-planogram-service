package com.demo.planogramservice.events.exception;

/**
 * Custom runtime exception representing transient / retryable order processing failures
 * (e.g., temporary database timeout, lock contention, external service unavailable).
 *
 * When thrown inside a KafkaListener, DefaultErrorHandler will trigger its BackOff retry policy
 * and retry consuming the record up to the configured max attempts.
 */
public class OrderProcessingRetryableException extends RuntimeException {

    public OrderProcessingRetryableException(String message) {
        super(message);
    }

    public OrderProcessingRetryableException(String message, Throwable cause) {
        super(message, cause);
    }
}
