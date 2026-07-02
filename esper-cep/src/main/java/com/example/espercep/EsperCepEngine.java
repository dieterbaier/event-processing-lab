package com.example.espercep;

import com.example.eventmodel.*;
import com.espertech.esper.client.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Esper CEP Engine for processing order lifecycle events.
 * Demonstrates Complex Event Processing using Esper's EPL (Event Processing Language).
 * [semantic-anchor: component.esper-cep]
 * [semantic-anchor: technology.esper]
 */
public class EsperCepEngine {
    
    private static final Logger logger = LoggerFactory.getLogger(EsperCepEngine.class);
    
    private final KafkaConsumer<String, String> kafkaConsumer;
    private final EPSPStatementManager epService;
    private final String topic;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final Map<String, Event> eventCache = new HashMap<>();
    
    // Statistics
    private long totalEventsReceived = 0;
    private long patternsDetected = 0;
    
    /**
     * Create an EsperCepEngine with the specified Kafka configuration.
     * @param bootstrapServers Kafka bootstrap servers
     * @param topic Kafka topic to consume from
     * @param groupId Consumer group ID
     */
    public EsperCepEngine(String bootstrapServers, String topic, String groupId) {
        this.topic = topic;
        
        // Configure Kafka consumer [semantic-anchor: technology.kafka]
        Properties kafkaProps = new Properties();
        kafkaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        kafkaProps.put(ConsumerConfig.CLIENT_ID_CONFIG, "esper-cep-engine");
        kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        kafkaProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        kafkaProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        kafkaProps.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "10000");
        
        this.kafkaConsumer = new KafkaConsumer<>(kafkaProps);
        this.kafkaConsumer.subscribe(Collections.singletonList(topic));
        
        // Initialize Esper engine [semantic-anchor: technology.esper]
        this.epService = initializeEsperEngine();
        
        logger.info("EsperCepEngine initialized for topic: {} with group: {}", topic, groupId);
    }
    
    /**
     * Initialize the Esper engine and register EPL statements.
     * @return the configured EPSPStatementManager
     */
    private EPSPStatementManager initializeEsperEngine() {
        // Create Esper configuration
        Configuration configuration = new Configuration();
        configuration.setEngineName("EventProcessingEngine");
        
        // Configure time handling
        configuration.getEngineDefaults().getThreading().setThreadingModel(Configuration.EngineSettings.ThreadingModel.SINGLETHREAD);
        
        // Create Esper service provider
        EPServiceProvider epService = EPServiceManager.getProvider(configuration);
        
        // Register event types for polymorphic JSON events
        registerEventTypes(epService);
        
        // Create statement manager
        EPSPStatementManager epStatementManager = epService.getEPSPManager();
        
        // Register all EPL statements for our use cases
        registerEplStatements(epStatementManager);
        
        logger.info("Esper engine initialized with {} statements", epStatementManager.getStatementNames().size());
        
        return epStatementManager;
    }
    
    /**
     * Register event types with Esper for polymorphic event handling.
     * @param epService the Esper service provider
     */
    private void registerEventTypes(EPServiceProvider epService) {
        // In Esper, we need to register the Java classes as event types
        // The JSON will be deserialized to these classes
        ConfigurationEventType eventType = new ConfigurationEventType();
        
        // Register OrderCreated
        eventType.setClassName(OrderCreated.class.getName());
        epService.getEPAdministrator().getConfiguration().addEventType("OrderCreated", eventType);
        
        // Register PaymentReceived
        eventType = new ConfigurationEventType();
        eventType.setClassName(PaymentReceived.class.getName());
        epService.getEPAdministrator().getConfiguration().addEventType("PaymentReceived", eventType);
        
        // Register PaymentFailed
        eventType = new ConfigurationEventType();
        eventType.setClassName(PaymentFailed.class.getName());
        epService.getEPAdministrator().getConfiguration().addEventType("PaymentFailed", eventType);
        
        // Register OrderCancelled
        eventType = new ConfigurationEventType();
        eventType.setClassName(OrderCancelled.class.getName());
        epService.getEPAdministrator().getConfiguration().addEventType("OrderCancelled", eventType);
        
        // Register ShipmentStarted
        eventType = new ConfigurationEventType();
        eventType.setClassName(ShipmentStarted.class.getName());
        epService.getEPAdministrator().getConfiguration().addEventType("ShipmentStarted", eventType);
        
        // Register base Event for generic handling
        eventType = new ConfigurationEventType();
        eventType.setClassName(Event.class.getName());
        epService.getEPAdministrator().getConfiguration().addEventType("Event", eventType);
        
        logger.info("Registered event types with Esper engine");
    }
    
    /**
     * Register EPL statements for various event processing scenarios.
     * @param epStatementManager the Esper statement manager
     */
    private void registerEplStatements(EPSPStatementManager epStatementManager) {
        // Create EPL statements as String objects
        String[] eplStatements = {
            // Order Creation Detection [semantic-anchor: scenario.order-creation]
            createOrderCreationEpl(),
            
            // Payment Processing Detection [semantic-anchor: scenario.payment-processing]
            createPaymentProcessingEpl(),
            
            // Payment Timeout Detection [semantic-anchor: scenario.payment-timeout]
            createPaymentTimeoutEpl(),
            
            // Order Cancellation Detection [semantic-anchor: scenario.order-cancellation]
            createOrderCancellationEpl(),
            
            // Shipment Start Detection [semantic-anchor: scenario.shipment-start]
            createShipmentStartEpl(),
            
            // Complete Order Flow Detection
            createCompleteOrderFlowEpl(),
            
            // Payment Failure Pattern Detection
            createPaymentFailurePatternEpl(),
            
            // Order State Machine
            createOrderStateMachineEpl()
        };
        
        // Register each EPL statement with a listener
        for (String epl : eplStatements) {
            try {
                EPSPStatement statement = epStatementManager.createEPL(epl);
                statement.addListener((newData, oldData) -> {
                    processEsperResults(newData, oldData, statement.getName());
                });
                logger.info("Registered EPL statement: {}", statement.getName());
            } catch (Exception e) {
                logger.error("Failed to create EPL statement: " + e.getMessage(), e);
            }
        }
    }
    
    /**
     * EPL for Order Creation Detection [semantic-anchor: scenario.order-creation]
     */
    private String createOrderCreationEpl() {
        return "@name('order-creation-detection') " +
               "SELECT * FROM OrderCreated";
    }
    
    /**
     * EPL for Payment Processing Detection [semantic-anchor: scenario.payment-processing]
     */
    private String createPaymentProcessingEpl() {
        return "@name('payment-processing-detection') " +
               "SELECT oc.orderId, pr.paymentId, pr.amount, pr.paymentMethod " +
               "FROM pattern[every oc=OrderCreated -> pr=PaymentReceived(oc.orderId = pr.orderId)]";
    }
    
    /**
     * EPL for Payment Timeout Detection [semantic-anchor: scenario.payment-timeout]
     */
    private String createPaymentTimeoutEpl() {
        // Detect orders that have not received payment within 5 minutes
        return "@name('payment-timeout-detection') " +
               "SELECT oc.orderId, oc.timestamp as orderTimestamp " +
               "FROM OrderCreated as oc " +
               "WHERE NOT EXISTS (PaymentReceived(orderId = oc.orderId)) " +
               "AND timer:within(5 min) " +
               "OUTPUT ALL EVERY 1 min";
    }
    
    /**
     * EPL for Order Cancellation Detection [semantic-anchor: scenario.order-cancellation]
     */
    private String createOrderCancellationEpl() {
        return "@name('order-cancellation-detection') " +
               "SELECT * FROM OrderCancelled";
    }
    
    /**
     * EPL for Shipment Start Detection [semantic-anchor: scenario.shipment-start]
     */
    private String createShipmentStartEpl() {
        // Detect shipment that follows successful payment
        return "@name('shipment-start-detection') " +
               "SELECT ss.orderId, ss.shipmentId, ss.trackingNumber, pr.paymentId " +
               "FROM pattern[every pr=PaymentReceived -> ss=ShipmentStarted(pr.orderId = ss.orderId)]";
    }
    
    /**
     * EPL for Complete Order Flow Detection
     */
    private String createCompleteOrderFlowEpl() {
        // Detect complete order flow: OrderCreated -> PaymentReceived -> ShipmentStarted
        return "@name('complete-order-flow') " +
               "SELECT oc.orderId, oc.timestamp as orderTime, " +
               "       pr.paymentId, pr.timestamp as paymentTime, " +
               "       ss.shipmentId, ss.timestamp as shipmentTime " +
               "FROM pattern[every oc=OrderCreated -> pr=PaymentReceived(oc.orderId = pr.orderId) -> " +
               "ss=ShipmentStarted(pr.orderId = ss.orderId)]";
    }
    
    /**
     * EPL for Payment Failure Pattern Detection
     */
    private String createPaymentFailurePatternEpl() {
        // Detect payment failure pattern: OrderCreated -> PaymentFailed
        return "@name('payment-failure-pattern') " +
               "SELECT oc.orderId, pf.paymentId, pf.failureReason, pf.failureCode " +
               "FROM pattern[every oc=OrderCreated -> pf=PaymentFailed(oc.orderId = pf.orderId)]";
    }
    
    /**
     * EPL for Order State Machine
     */
    private String createOrderStateMachineEpl() {
        // Track order state transitions
        return "@name('order-state-machine') " +
               "SELECT orderId, 
" +
               "       CASE WHEN oc IS NOT NULL THEN 'CREATED' 
" +
               "            WHEN pr IS NOT NULL THEN 'PAID' 
" +
               "            WHEN ss IS NOT NULL THEN 'SHIPPED' 
" +
               "            WHEN oc_cancel IS NOT NULL THEN 'CANCELLED' 
" +
               "            ELSE 'UNKNOWN' END as state, 
" +
               "       latestEvent.timestamp as timestamp " +
               "FROM pattern[every latestEvent=Event -> 
" +
               "       (oc=OrderCreated(orderId = latestEvent.getOrderId()) OR 
" +
               "        pr=PaymentReceived(orderId = latestEvent.getOrderId()) OR 
" +
               "        ss=ShipmentStarted(orderId = latestEvent.getOrderId()) OR 
" +
               "        oc_cancel=OrderCancelled(orderId = latestEvent.getOrderId()))] " +
               "OUTPUT ALL EVERY 1 EVENT";
    }
    
    /**
     * Process results from Esper EPL statements.
     * @param newData new events matching the pattern
     * @param oldData old events (not used in this case)
     * @param statementName the name of the EPL statement
     */
    private void processEsperResults(Object[] newData, Object[] oldData, String statementName) {
        if (newData == null || newData.length == 0) {
            return;
        }
        
        patternsDetected++;
        
        switch (statementName) {
            case "order-creation-detection":
                processOrderCreationDetection(newData);
                break;
            case "payment-processing-detection":
                processPaymentProcessingDetection(newData);
                break;
            case "payment-timeout-detection":
                processPaymentTimeoutDetection(newData);
                break;
            case "order-cancellation-detection":
                processOrderCancellationDetection(newData);
                break;
            case "shipment-start-detection":
                processShipmentStartDetection(newData);
                break;
            case "complete-order-flow":
                processCompleteOrderFlow(newData);
                break;
            case "payment-failure-pattern":
                processPaymentFailurePattern(newData);
                break;
            case "order-state-machine":
                processOrderStateMachine(newData);
                break;
            default:
                logger.info("Pattern detected by '{}': {} events", statementName, newData.length);
        }
    }
    
    /**
     * Process order creation detection results.
     */
    private void processOrderCreationDetection(Object[] newData) {
        for (Object data : newData) {
            if (data instanceof OrderCreated) {
                OrderCreated order = (OrderCreated) data;
                logger.info("ESPER: Order Created detected - Order ID: {}, Customer: {}, Amount: {}",
                    order.getOrderId(), order.getCustomerId(), order.getTotalAmount());
                
                // Cache the order
                cacheEvent(order.getOrderId(), order);
            }
        }
    }
    
    /**
     * Process payment processing detection results.
     */
    private void processPaymentProcessingDetection(Object[] newData) {
        for (Object data : newData) {
            if (data instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> result = (Map<String, Object>) data;
                String orderId = (String) result.get("orderId");
                String paymentId = (String) result.get("paymentId");
                
                logger.info("ESPER: Payment Processing detected - Order ID: {}, Payment ID: {}",
                    orderId, paymentId);
            }
        }
    }
    
    /**
     * Process payment timeout detection results.
     */
    private void processPaymentTimeoutDetection(Object[] newData) {
        for (Object data : newData) {
            if (data instanceof OrderCreated) {
                OrderCreated order = (OrderCreated) data;
                logger.warn("ESPER: Payment Timeout detected - Order ID: {} has not received payment within 5 minutes",
                    order.getOrderId());
                
                // This could trigger timeout handling logic
            }
        }
    }
    
    /**
     * Process order cancellation detection results.
     */
    private void processOrderCancellationDetection(Object[] newData) {
        for (Object data : newData) {
            if (data instanceof OrderCancelled) {
                OrderCancelled order = (OrderCancelled) data;
                logger.warn("ESPER: Order Cancellation detected - Order ID: {}, Reason: {}",
                    order.getOrderId(), order.getCancellationReason());
            }
        }
    }
    
    /**
     * Process shipment start detection results.
     */
    private void processShipmentStartDetection(Object[] newData) {
        for (Object data : newData) {
            if (data instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> result = (Map<String, Object>) data;
                String orderId = (String) result.get("orderId");
                String shipmentId = (String) result.get("shipmentId");
                String trackingNumber = (String) result.get("trackingNumber");
                String paymentId = (String) result.get("paymentId");
                
                logger.info("ESPER: Shipment Start detected - Order ID: {}, Shipment ID: {}, Tracking: {}, Payment: {}",
                    orderId, shipmentId, trackingNumber, paymentId);
            }
        }
    }
    
    /**
     * Process complete order flow detection results.
     */
    private void processCompleteOrderFlow(Object[] newData) {
        for (Object data : newData) {
            if (data instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> result = (Map<String, Object>) data;
                String orderId = (String) result.get("orderId");
                
                logger.info("ESPER: Complete Order Flow detected - Order ID: {} processed from creation to shipment",
                    orderId);
            }
        }
    }
    
    /**
     * Process payment failure pattern detection results.
     */
    private void processPaymentFailurePattern(Object[] newData) {
        for (Object data : newData) {
            if (data instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> result = (Map<String, Object>) data;
                String orderId = (String) result.get("orderId");
                String paymentId = (String) result.get("paymentId");
                String failureReason = (String) result.get("failureReason");
                String failureCode = (String) result.get("failureCode");
                
                logger.warn("ESPER: Payment Failure Pattern detected - Order ID: {}, Payment ID: {}, Reason: {}, Code: {}",
                    orderId, paymentId, failureReason, failureCode);
            }
        }
    }
    
    /**
     * Process order state machine results.
     */
    private void processOrderStateMachine(Object[] newData) {
        for (Object data : newData) {
            if (data instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> result = (Map<String, Object>) data;
                String orderId = (String) result.get("orderId");
                String state = (String) result.get("state");
                
                logger.info("ESPER: Order State Machine - Order ID: {} transitioned to state: {}",
                    orderId, state);
            }
        }
    }
    
    /**
     * Cache an event for potential correlation.
     */
    private void cacheEvent(String key, Event event) {
        eventCache.put(key, event);
    }
    
    /**
     * Start consuming events and processing them through Esper.
     */
    public void start() {
        if (running.compareAndSet(false, true)) {
            logger.info("Starting Esper CEP Engine...");
            
            // Add shutdown hook
            Runtime.getRuntime().addShutdownHook(new Thread(this::stop));
            
            // Main consumption loop
            try {
                while (running.get()) {
                    // Poll for new events
                    ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1000));
                    
                    if (records.isEmpty()) {
                        logger.debug("No new events received");
                        continue;
                    }
                    
                    totalEventsReceived += records.count();
                    logger.info("Received {} events from Kafka (Total: {})", records.count(), totalEventsReceived);
                    
                    // Process each record through Esper
                    for (ConsumerRecord<String, String> record : records) {
                        processRecordWithEsper(record);
                    }
                    
                    // Manually commit offsets
                    kafkaConsumer.commitAsync();
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
            logger.warn("Esper CEP Engine is already running");
        }
    }
    
    /**
     * Process a Kafka record by sending the event to Esper.
     * @param record the Kafka record to process
     */
    private void processRecordWithEsper(ConsumerRecord<String, String> record) {
        try {
            String key = record.key();
            String value = record.value();
            long offset = record.offset();
            
            // Deserialize JSON to Event object [semantic-anchor: decision.json-serialization]
            Event event = EventSerializer.deserialize(value);
            
            // Log the incoming event
            logger.debug("Processing event {} [key={}, offset={}]", event.getEventId(), key, offset);
            
            // Send the event to Esper for pattern detection
            sendToEsper(event);
            
            // Cache the event for reference
            cacheEvent(key, event);
            
        } catch (EventSerializer.EventDeserializationException e) {
            logger.error("Failed to deserialize event from record [partition={}, offset={}]: {}", 
                record.partition(), record.offset(), e.getMessage());
        } catch (Exception e) {
            logger.error("Error processing record with Esper [partition={}, offset={}]: {}", 
                record.partition(), record.offset(), e.getMessage());
        }
    }
    
    /**
     * Send an event to Esper for processing.
     * @param event the event to send
     */
    private void sendToEsper(Event event) {
        try {
            // Send the event to Esper based on its type
            switch (event.getEventTypeEnum()) {
                case ORDER_CREATED:
                    epService.route((OrderCreated) event);
                    break;
                case PAYMENT_RECEIVED:
                    epService.route((PaymentReceived) event);
                    break;
                case PAYMENT_FAILED:
                    epService.route((PaymentFailed) event);
                    break;
                case ORDER_CANCELLED:
                    epService.route((OrderCancelled) event);
                    break;
                case SHIPMENT_STARTED:
                    epService.route((ShipmentStarted) event);
                    break;
                default:
                    logger.warn("Unknown event type for Esper: {}", event.getEventType());
            }
        } catch (Exception e) {
            logger.error("Error sending event to Esper: " + e.getMessage(), e);
        }
    }
    
    /**
     * Stop the Esper CEP Engine.
     */
    public void stop() {
        if (running.compareAndSet(true, false)) {
            logger.info("Stopping Esper CEP Engine...");
            
            try {
                kafkaConsumer.wakeup();
            } catch (Exception e) {
                logger.error("Error during shutdown: " + e.getMessage(), e);
            } finally {
                kafkaConsumer.close();
                eventCache.clear();
                
                // Print final statistics
                printStatistics();
                
                logger.info("Esper CEP Engine stopped");
            }
        }
    }
    
    /**
     * Print final statistics.
     */
    private void printStatistics() {
        logger.info("=== Esper CEP Engine Statistics ===");
        logger.info("Total Events Received: {}", totalEventsReceived);
        logger.info("Patterns Detected: {}", patternsDetected);
        logger.info("Events in Cache: {}", eventCache.size());
        logger.info("=======================================");
    }
    
    /**
     * Get the Kafka topic being consumed from.
     * @return the Kafka topic
     */
    public String getTopic() {
        return topic;
    }
    
    /**
     * Check if the engine is currently running.
     * @return true if running, false otherwise
     */
    public boolean isRunning() {
        return running.get();
    }
    
    /**
     * Get the total number of events received.
     * @return the total events received
     */
    public long getTotalEventsReceived() {
        return totalEventsReceived;
    }
    
    /**
     * Get the number of patterns detected.
     * @return the number of patterns detected
     */
    public long getPatternsDetected() {
        return patternsDetected;
    }
    
    /**
     * Main method to run the Esper CEP Engine from command line.
     * @param args command line arguments: [bootstrap-servers] [topic] [group-id]
     */
    public static void main(String[] args) {
        String bootstrapServers = "localhost:9092";
        String topic = "order-events";
        String groupId = "esper-cep-group";
        
        if (args.length > 0) {
            bootstrapServers = args[0];
        }
        if (args.length > 1) {
            topic = args[1];
        }
        if (args.length > 2) {
            groupId = args[2];
        }
        
        logger.info("Starting Esper CEP Engine");
        logger.info("Bootstrap Servers: {}", bootstrapServers);
        logger.info("Topic: {}", topic);
        logger.info("Group ID: {}", groupId);
        
        EsperCepEngine engine = new EsperCepEngine(bootstrapServers, topic, groupId);
        
        // Start the engine
        engine.start();
    }
}