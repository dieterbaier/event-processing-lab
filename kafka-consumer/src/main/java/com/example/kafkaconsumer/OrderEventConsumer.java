package com.example.kafkaconsumer;

import com.example.eventmodel.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Basic Kafka consumer for order lifecycle events.
 * Consumes events from Kafka and processes them, demonstrating basic event processing.
 * [semantic-anchor: component.kafka-consumer]
 * [semantic-anchor: technology.kafka]
 */
public class OrderEventConsumer {
    
    private static final Logger logger = LoggerFactory.getLogger(OrderEventConsumer.class);
    
    private final KafkaConsumer<String, String> consumer;
    private final String topic;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final Map<String, Event> eventCache = new HashMap<>();
    
    /**
     * Create an OrderEventConsumer with the specified Kafka configuration.
     * @param bootstrapServers Kafka bootstrap servers
     * @param topic Kafka topic to consume from
     * @param groupId Consumer group ID
     */
    public OrderEventConsumer(String bootstrapServers, String topic, String groupId) {
        this.topic = topic;
        
        // Configure Kafka consumer
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "kafka-consumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "10000");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500");
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "300000");
        
        this.consumer = new KafkaConsumer<>(props);
        this.consumer.subscribe(Collections.singletonList(topic));
        
        logger.info("OrderEventConsumer initialized for topic: {} with group: {}", topic, groupId);
    }
    
    /**
     * Start consuming events.
     */
    public void start() {
        if (running.compareAndSet(false, true)) {
            logger.info("Starting event consumption from topic: {}", topic);
            
            // Add shutdown hook
            Runtime.getRuntime().addShutdownHook(new Thread(this::stop));
            
            // Main consumption loop
            try {
                while (running.get()) {
                    // Poll for new events
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                    
                    if (records.isEmpty()) {
                        logger.debug("No new events received");
                        continue;
                    }
                    
                    logger.info("Received {} events from Kafka", records.count());
                    
                    // Process each record
                    for (ConsumerRecord<String, String> record : records) {
                        processRecord(record);
                    }
                    
                    // Manually commit offsets
                    consumer.commitAsync();
                }
            } catch (WakeupException e) {
                // Consumer was woken up, typically during shutdown
                logger.info("Consumer woken up, shutting down gracefully");
            } catch (Exception e) {
                logger.error("Error in consumer loop: " + e.getMessage(), e);
            } finally {
                stop();
            }
        } else {
            logger.warn("Consumer is already running");
        }
    }
    
    /**
     * Process a single Kafka record.
     * @param record the Kafka record to process
     */
    private void processRecord(ConsumerRecord<String, String> record) {
        try {
            String key = record.key();
            String value = record.value();
            String partition = String.valueOf(record.partition());
            long offset = record.offset();
            long timestamp = record.timestamp();
            
            logger.debug("Processing record [partition={}, offset={}, key={}, timestamp={}]", 
                partition, offset, key, timestamp);
            
            // Deserialize JSON to Event object [semantic-anchor: decision.json-serialization]
            Event event = EventSerializer.deserialize(value);
            
            // Log the event
            logEvent(event, partition, offset, key);
            
            // Cache the event for potential correlation
            cacheEvent(key, event);
            
            // Process the event based on its type
            processEvent(event, partition, offset, key);
            
        } catch (EventSerializer.EventDeserializationException e) {
            logger.error("Failed to deserialize event from record [partition={}, offset={}]: {}", 
                record.partition(), record.offset(), e.getMessage());
        } catch (Exception e) {
            logger.error("Error processing record [partition={}, offset={}]: {}", 
                record.partition(), record.offset(), e.getMessage());
        }
    }
    
    /**
     * Log an event with its metadata.
     * @param event the event to log
     * @param partition the Kafka partition
     * @param offset the Kafka offset
     * @param key the message key
     */
    private void logEvent(Event event, String partition, long offset, String key) {
        switch (event.getEventTypeEnum()) {
            case ORDER_CREATED:
                OrderCreated orderCreated = (OrderCreated) event;
                logger.info("ORDER_CREATED [partition={}, offset={}, key={}] - " +
                          "Order: {}, Customer: {}, Amount: {}, Items: {}",
                    partition, offset, key,
                    orderCreated.getOrderId(), 
                    orderCreated.getCustomerId(), 
                    orderCreated.getTotalAmount(),
                    orderCreated.getOrderItems().size());
                break;
                
            case PAYMENT_RECEIVED:
                PaymentReceived paymentReceived = (PaymentReceived) event;
                logger.info("PAYMENT_RECEIVED [partition={}, offset={}, key={}] - " +
                          "Order: {}, Payment: {}, Amount: {}, Method: {}, Gateway: {}",
                    partition, offset, key,
                    paymentReceived.getOrderId(),
                    paymentReceived.getPaymentId(),
                    paymentReceived.getAmount(),
                    paymentReceived.getPaymentMethod(),
                    paymentReceived.getPaymentGateway());
                break;
                
            case PAYMENT_FAILED:
                PaymentFailed paymentFailed = (PaymentFailed) event;
                logger.warn("PAYMENT_FAILED [partition={}, offset={}, key={}] - " +
                          "Order: {}, Payment: {}, Amount: {}, Reason: {}, Code: {}",
                    partition, offset, key,
                    paymentFailed.getOrderId(),
                    paymentFailed.getPaymentId(),
                    paymentFailed.getAmount(),
                    paymentFailed.getFailureReason(),
                    paymentFailed.getFailureCode());
                break;
                
            case ORDER_CANCELLED:
                OrderCancelled orderCancelled = (OrderCancelled) event;
                logger.warn("ORDER_CANCELLED [partition={}, offset={}, key={}] - " +
                          "Order: {}, Reason: {}, Refund Eligible: {}, Cancelled By: {}",
                    partition, offset, key,
                    orderCancelled.getOrderId(),
                    orderCancelled.getCancellationReason(),
                    orderCancelled.isRefundEligible(),
                    orderCancelled.getCancelledBy());
                break;
                
            case SHIPMENT_STARTED:
                ShipmentStarted shipmentStarted = (ShipmentStarted) event;
                logger.info("SHIPMENT_STARTED [partition={}, offset={}, key={}] - " +
                          "Order: {}, Shipment: {}, Tracking: {}, Carrier: {}, Service: {}",
                    partition, offset, key,
                    shipmentStarted.getOrderId(),
                    shipmentStarted.getShipmentId(),
                    shipmentStarted.getTrackingNumber(),
                    shipmentStarted.getCarrier(),
                    shipmentStarted.getCarrierService());
                break;
                
            default:
                logger.info("UNKNOWN_EVENT [partition={}, offset={}, key={}] - " +
                          "Type: {}, Event ID: {}",
                    partition, offset, key,
                    event.getEventType(),
                    event.getEventId());
        }
    }
    
    /**
     * Cache an event for potential correlation with future events.
     * @param key the cache key (typically order ID)
     * @param event the event to cache
     */
    private void cacheEvent(String key, Event event) {
        eventCache.put(key, event);
        logger.debug("Cached event {} with key: {}", event.getEventId(), key);
    }
    
    /**
     * Process an event based on its type.
     * This method demonstrates basic event processing logic.
     * @param event the event to process
     * @param partition the Kafka partition
     * @param offset the Kafka offset
     * @param key the message key
     */
    private void processEvent(Event event, String partition, long offset, String key) {
        switch (event.getEventTypeEnum()) {
            case ORDER_CREATED:
                processOrderCreated((OrderCreated) event, partition, offset, key);
                break;
            case PAYMENT_RECEIVED:
                processPaymentReceived((PaymentReceived) event, partition, offset, key);
                break;
            case PAYMENT_FAILED:
                processPaymentFailed((PaymentFailed) event, partition, offset, key);
                break;
            case ORDER_CANCELLED:
                processOrderCancelled((OrderCancelled) event, partition, offset, key);
                break;
            case SHIPMENT_STARTED:
                processShipmentStarted((ShipmentStarted) event, partition, offset, key);
                break;
            default:
                logger.warn("Unknown event type: {}", event.getEventType());
        }
    }
    
    /**
     * Process OrderCreated event.
     * [semantic-anchor: scenario.order-creation]
     */
    private void processOrderCreated(OrderCreated event, String partition, long offset, String key) {
        // This is the start of an order lifecycle
        logger.info("Processing OrderCreated for order: {}", event.getOrderId());
        
        // Validate the order
        if (event.getOrderItems() == null || event.getOrderItems().isEmpty()) {
            logger.warn("Order {} has no items", event.getOrderId());
        }
        
        // Check total amount consistency
        BigDecimal calculatedTotal = event.getOrderItems().stream()
            .map(OrderCreated.OrderItem::getTotalPrice)
            .reduce(BigDecimal.ZERO, BigDecimal::add);
        
        if (!calculatedTotal.equals(event.getTotalAmount())) {
            logger.warn("Order {} total amount mismatch: expected {}, actual {}", 
                event.getOrderId(), calculatedTotal, event.getTotalAmount());
        }
        
        // Store the order for future reference
        eventCache.put(event.getOrderId(), event);
    }
    
    /**
     * Process PaymentReceived event.
     * [semantic-anchor: scenario.payment-processing]
     */
    private void processPaymentReceived(PaymentReceived event, String partition, long offset, String key) {
        logger.info("Processing PaymentReceived for order: {}", event.getOrderId());
        
        // Check if we have the corresponding order
        Event cachedOrder = eventCache.get(event.getOrderId());
        if (cachedOrder instanceof OrderCreated) {
            OrderCreated order = (OrderCreated) cachedOrder;
            
            // Verify payment amount matches order total
            if (!event.getAmount().equals(order.getTotalAmount())) {
                logger.warn("Payment amount {} does not match order total {} for order {}", 
                    event.getAmount(), order.getTotalAmount(), event.getOrderId());
            }
            
            // Payment processing complete - order can be fulfilled
            logger.info("Payment processed successfully for order {}", event.getOrderId());
            
        } else {
            logger.warn("Received payment for unknown order: {}", event.getOrderId());
        }
    }
    
    /**
     * Process PaymentFailed event.
     * [semantic-anchor: scenario.payment-processing]
     */
    private void processPaymentFailed(PaymentFailed event, String partition, long offset, String key) {
        logger.warn("Processing PaymentFailed for order: {}", event.getOrderId());
        
        // Payment failed - order should be handled appropriately
        logger.info("Payment failed for order {}: {} ({})", 
            event.getOrderId(), event.getFailureReason(), event.getFailureCode());
        
        // This might trigger order cancellation or retry logic
    }
    
    /**
     * Process OrderCancelled event.
     * [semantic-anchor: scenario.order-cancellation]
     */
    private void processOrderCancelled(OrderCancelled event, String partition, long offset, String key) {
        logger.warn("Processing OrderCancelled for order: {}", event.getOrderId());
        
        // Order cancellation processing
        if (event.isRefundEligible()) {
            logger.info("Order {} is eligible for refund", event.getOrderId());
        } else {
            logger.info("Order {} is NOT eligible for refund", event.getOrderId());
        }
        
        // Clean up cached data
        eventCache.remove(event.getOrderId());
    }
    
    /**
     * Process ShipmentStarted event.
     * [semantic-anchor: scenario.shipment-start]
     */
    private void processShipmentStarted(ShipmentStarted event, String partition, long offset, String key) {
        logger.info("Processing ShipmentStarted for order: {}", event.getOrderId());
        
        // Shipment processing
        logger.info("Shipment {} started for order {} with tracking number: {}", 
            event.getShipmentId(), event.getOrderId(), event.getTrackingNumber());
        
        // This could trigger notification to customer, etc.
    }
    
    /**
     * Stop consuming events.
     */
    public void stop() {
        if (running.compareAndSet(true, false)) {
            logger.info("Stopping consumer...");
            
            try {
                consumer.wakeup(); // This will cause the poll() to throw WakeupException
            } catch (Exception e) {
                logger.error("Error during consumer shutdown: " + e.getMessage(), e);
            } finally {
                consumer.close();
                eventCache.clear();
                logger.info("Consumer stopped");
            }
        }
    }
    
    /**
     * Get the Kafka topic being consumed from.
     * @return the Kafka topic
     */
    public String getTopic() {
        return topic;
    }
    
    /**
     * Check if the consumer is currently running.
     * @return true if running, false otherwise
     */
    public boolean isRunning() {
        return running.get();
    }
    
    /**
     * Get the current cache size.
     * @return the number of cached events
     */
    public int getCacheSize() {
        return eventCache.size();
    }
    
    /**
     * Get statistics about processed events.
     * @return a map with event type counts
     */
    public Map<String, Long> getEventStatistics() {
        Map<String, Long> stats = new HashMap<>();
        stats.put("ORDER_CREATED", eventCache.values().stream()
            .filter(e -> e.getEventTypeEnum() == EventType.ORDER_CREATED)
            .count());
        stats.put("PAYMENT_RECEIVED", eventCache.values().stream()
            .filter(e -> e.getEventTypeEnum() == EventType.PAYMENT_RECEIVED)
            .count());
        stats.put("PAYMENT_FAILED", eventCache.values().stream()
            .filter(e -> e.getEventTypeEnum() == EventType.PAYMENT_FAILED)
            .count());
        stats.put("ORDER_CANCELLED", eventCache.values().stream()
            .filter(e -> e.getEventTypeEnum() == EventType.ORDER_CANCELLED)
            .count());
        stats.put("SHIPMENT_STARTED", eventCache.values().stream()
            .filter(e -> e.getEventTypeEnum() == EventType.SHIPMENT_STARTED)
            .count());
        
        return stats;
    }
    
    /**
     * Main method to run the consumer from command line.
     * @param args command line arguments: [bootstrap-servers] [topic] [group-id]
     */
    public static void main(String[] args) {
        String bootstrapServers = "localhost:9092";
        String topic = "order-events";
        String groupId = "kafka-consumer-group";
        
        if (args.length > 0) {
            bootstrapServers = args[0];
        }
        if (args.length > 1) {
            topic = args[1];
        }
        if (args.length > 2) {
            groupId = args[2];
        }
        
        logger.info("Starting OrderEventConsumer");
        logger.info("Bootstrap Servers: {}", bootstrapServers);
        logger.info("Topic: {}", topic);
        logger.info("Group ID: {}", groupId);
        
        OrderEventConsumer consumer = new OrderEventConsumer(bootstrapServers, topic, groupId);
        
        // Start consuming events
        consumer.start();
    }
}