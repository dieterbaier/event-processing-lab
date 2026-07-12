package com.example.eventproducer;

import com.example.eventmodel.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Kafka producer for order lifecycle events.
 * Generates and sends events to a Kafka topic for processing by various engines.
 * [semantic-anchor: component.event-producer]
 * [semantic-anchor: technology.kafka]
 */
public class OrderEventProducer {
    
    private static final Logger logger = LoggerFactory.getLogger(OrderEventProducer.class);
    
    private final KafkaProducer<String, String> producer;
    private final String topic;
    private final int eventsPerSecond;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private final Random random = new Random();
    
    /**
     * Create an OrderEventProducer with the specified Kafka configuration.
     * @param bootstrapServers Kafka bootstrap servers
     * @param topic Kafka topic to produce events to
     */
    public OrderEventProducer(String bootstrapServers, String topic) {
        this(bootstrapServers, topic, 10); // Default: 10 events per second
    }
    
    /**
     * Create an OrderEventProducer with the specified Kafka configuration and rate.
     * @param bootstrapServers Kafka bootstrap servers
     * @param topic Kafka topic to produce events to
     * @param eventsPerSecond Rate of event generation (events per second)
     */
    public OrderEventProducer(String bootstrapServers, String topic, int eventsPerSecond) {
        this.topic = topic;
        this.eventsPerSecond = eventsPerSecond;
        
        // Configure Kafka producer
        Properties props = new Properties();
        props.put(org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(org.apache.kafka.clients.producer.ProducerConfig.CLIENT_ID_CONFIG, "event-producer");
        props.put(org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG, "all");
        props.put(org.apache.kafka.clients.producer.ProducerConfig.RETRIES_CONFIG, 3);
        props.put(org.apache.kafka.clients.producer.ProducerConfig.LINGER_MS_CONFIG, 5);
        props.put(org.apache.kafka.clients.producer.ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(org.apache.kafka.clients.producer.ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(org.apache.kafka.clients.producer.ProducerConfig.MAX_BLOCK_MS_CONFIG, 60000);
        
        this.producer = new KafkaProducer<>(props);
        
        logger.info("OrderEventProducer initialized for topic: {} at rate: {} events/second", topic, eventsPerSecond);
    }
    
    /**
     * Start producing events at the configured rate.
     */
    public void start() {
        if (running.compareAndSet(false, true)) {
            logger.info("Starting event production...");
            
            // Calculate delay between events in milliseconds
            long delayMs = 1000L / eventsPerSecond;
            
            // Schedule event production at fixed rate
            scheduler.scheduleAtFixedRate(this::produceEvent, 0, delayMs, TimeUnit.MILLISECONDS);
            
            // Add shutdown hook
            Runtime.getRuntime().addShutdownHook(new Thread(this::stop));
        } else {
            logger.warn("Event producer is already running");
        }
    }
    
    /**
     * Stop producing events.
     */
    public void stop() {
        if (running.compareAndSet(true, false)) {
            logger.info("Stopping event production...");
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
            
            producer.close();
            logger.info("Event producer stopped");
        }
    }
    
    /**
     * Produce a single event.
     * This method is called by the scheduler to generate and send events.
     */
    private void produceEvent() {
        if (!running.get()) {
            return;
        }
        
        try {
            // Randomly select an event type to produce
            Event event = generateRandomEvent();
            
            // Serialize event to JSON [semantic-anchor: decision.json-serialization]
            String json = EventSerializer.serialize(event);
            
            // Use order ID as the key for partitioning
            String key = extractOrderId(event);
            
            // Send the event
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, json);
            
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    logger.error("Error sending event {} to topic {}: {}", 
                        event.getEventId(), topic, exception.getMessage());
                } else {
                    logger.debug("Sent event {} to topic {} [partition={}, offset={}]", 
                        event.getEventId(), topic, 
                        metadata.partition(), metadata.offset());
                    
                    // Log summary at info level
                    logger.info("Produced {} event for order {} to partition {}", 
                        event.getEventType(), key, metadata.partition());
                }
            });
            
            producer.flush(); // Ensure messages are sent immediately for learning
            
        } catch (Exception e) {
            logger.error("Error producing event: " + e.getMessage(), e);
        }
    }
    
    /**
     * Generate a random event based on probabilities.
     * This creates realistic event patterns for learning scenarios.
     * @return a randomly generated event
     */
    private Event generateRandomEvent() {
        int choice = random.nextInt(100);
        
        // 40% chance: Start a new order
        if (choice < 40) {
            return EventFactory.createRandomOrderCreated();
        }
        // 30% chance: Process payment for existing order
        else if (choice < 70) {
            return EventFactory.createRandomPaymentReceived(generateRandomOrderId());
        }
        // 10% chance: Payment failure
        else if (choice < 80) {
            return EventFactory.createRandomPaymentFailed(generateRandomOrderId());
        }
        // 10% chance: Order cancellation
        else if (choice < 90) {
            return EventFactory.createRandomOrderCancelled(generateRandomOrderId());
        }
        // 10% chance: Start shipment
        else {
            return EventFactory.createRandomShipmentStarted(generateRandomOrderId());
        }
    }
    
    /**
     * Extract order ID from an event for use as Kafka message key.
     * @param event the event
     * @return the order ID or event ID if order ID not available
     */
    private String extractOrderId(Event event) {
        if (event instanceof OrderCreated) {
            return ((OrderCreated) event).getOrderId();
        } else if (event instanceof PaymentReceived) {
            return ((PaymentReceived) event).getOrderId();
        } else if (event instanceof PaymentFailed) {
            return ((PaymentFailed) event).getOrderId();
        } else if (event instanceof OrderCancelled) {
            return ((OrderCancelled) event).getOrderId();
        } else if (event instanceof ShipmentStarted) {
            return ((ShipmentStarted) event).getOrderId();
        } else {
            return event.getEventId();
        }
    }
    
    /**
     * Generate a random order ID for existing orders.
     * @return a random order ID
     */
    private String generateRandomOrderId() {
        return "ORD-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
    }
    
    /**
     * Send a specific event to Kafka.
     * @param event the event to send
     * @return a Future that can be used to track the send operation
     */
    public Future<RecordMetadata> sendEvent(Event event) {
        String json = EventSerializer.serialize(event);
        String key = extractOrderId(event);
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, json);
        
        return producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                logger.error("Error sending event {} to topic {}: {}", 
                    event.getEventId(), topic, exception.getMessage());
            } else {
                logger.info("Sent event {} to topic {} [partition={}, offset={}]", 
                    event.getEventId(), topic, metadata.partition(), metadata.offset());
            }
        });
    }
    
    /**
     * Send a sequence of events representing a complete order lifecycle.
     * This is useful for testing specific scenarios.
     * [semantic-anchor: scenario.order-creation]
     * [semantic-anchor: scenario.payment-processing]
     * [semantic-anchor: scenario.shipment-start]
     */
    public void sendOrderLifecycleSequence() {
        Event[] events = EventFactory.createOrderLifecycleSequence();
        
        for (Event event : events) {
            sendEvent(event);
        }
        
        producer.flush();
        logger.info("Sent complete order lifecycle sequence");
    }
    
    /**
     * Send a sequence of events representing an order with failed payment.
     * [semantic-anchor: scenario.payment-processing]
     * [semantic-anchor: scenario.order-cancellation]
     */
    public void sendOrderWithFailedPaymentSequence() {
        Event[] events = EventFactory.createOrderWithFailedPaymentSequence();
        
        for (Event event : events) {
            sendEvent(event);
        }
        
        producer.flush();
        logger.info("Sent order with failed payment sequence");
    }
    
    /**
     * Get the Kafka topic being used.
     * @return the Kafka topic
     */
    public String getTopic() {
        return topic;
    }
    
    /**
     * Check if the producer is currently running.
     * @return true if running, false otherwise
     */
    public boolean isRunning() {
        return running.get();
    }
    
    /**
     * Main method to run the producer from command line.
     * @param args command line arguments: [bootstrap-servers] [topic] [rate]
     */
    public static void main(String[] args) {
        String bootstrapServers = "localhost:9092";
        String topic = "order-events";
        int rate = 10;
        
        if (args.length > 0) {
            bootstrapServers = args[0];
        }
        if (args.length > 1) {
            topic = args[1];
        }
        if (args.length > 2) {
            rate = Integer.parseInt(args[2]);
        }
        
        logger.info("Starting OrderEventProducer");
        logger.info("Bootstrap Servers: {}", bootstrapServers);
        logger.info("Topic: {}", topic);
        logger.info("Rate: {} events/second", rate);
        
        OrderEventProducer producer = new OrderEventProducer(bootstrapServers, topic, rate);
        
        // Start producing events
        producer.start();
        
        // Keep the application running
        try {
            while (producer.isRunning()) {
                Thread.sleep(1000);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}