package com.example.flinkcep;

import com.example.eventmodel.*;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cep.*;
import org.apache.flink.cep.functions.*;
import org.apache.flink.cep.pattern.*;
import org.apache.flink.cep.pattern.conditions.*;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Pattern;

/**
 * Apache Flink CEP processor for order lifecycle events.
 * Demonstrates Complex Event Processing using Flink's CEP library.
 * [semantic-anchor: component.flink-cep]
 * [semantic-anchor: technology.flink-cep]
 */
public class FlinkCepProcessor {
    
    private static final Logger logger = LoggerFactory.getLogger(FlinkCepProcessor.class);
    
    private final String bootstrapServers;
    private final String topic;
    private final String groupId;
    
    /**
     * Create a FlinkCepProcessor with the specified Kafka configuration.
     * @param bootstrapServers Kafka bootstrap servers
     * @param topic Kafka topic to consume from
     * @param groupId Consumer group ID
     */
    public FlinkCepProcessor(String bootstrapServers, String topic, String groupId) {
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
        this.groupId = groupId;
    }
    
    /**
     * Create and configure the Kafka source.
     * @return the configured KafkaSource
     */
    private KafkaSource<String> createKafkaSource() {
        Properties kafkaProps = new Properties();
        kafkaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        kafkaProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        kafkaProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        
        return KafkaSource.<String>builder()
            .setBootstrapServers(bootstrapServers)
            .setTopics(topic)
            .setGroupId(groupId)
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            .setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
            .build();
    }
    
    /**
     * Deserialize JSON string to Event object.
     */
    private static class EventDeserializer implements org.apache.flink.api.common.functions.MapFunction<String, Event> {
        @Override
        public Event map(String value) throws Exception {
            return EventSerializer.deserialize(value);
        }
    }
    
    /**
     * Extract order ID from event for keying.
     */
    private static class OrderIdExtractor implements KeySelector<Event, String> {
        @Override
        public String getKey(Event event) {
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
    }
    
    /**
     * Create a pattern for Order Creation detection.
     * [semantic-anchor: scenario.order-creation]
     */
    private Pattern<Event, ?> createOrderCreationPattern() {
        return Pattern.<Event>begin("order-created")
            .where(new SimpleCondition<Event>() {
                @Override
                public boolean filter(Event event) {
                    return event instanceof OrderCreated;
                }
            })
            .build();
    }
    
    /**
     * Create a pattern for Payment Processing detection.
     * [semantic-anchor: scenario.payment-processing]
     */
    private Pattern<Event, ?> createPaymentProcessingPattern() {
        return Pattern.<Event>begin("order-created")
            .where(new SimpleCondition<Event>() {
                @Override
                public boolean filter(Event event) {
                    return event instanceof OrderCreated;
                }
            })
            .next("payment-received")
            .where(new SimpleCondition<Event>() {
                @Override
                public boolean filter(Event event) {
                    return event instanceof PaymentReceived;
                }
            })
            .within(Time.minutes(10)) // Payment should happen within 10 minutes
            .build();
    }
    
    /**
     * Create a pattern for Payment Timeout detection.
     * [semantic-anchor: scenario.payment-timeout]
     */
    private Pattern<Event, ?> createPaymentTimeoutPattern() {
        return Pattern.<Event>begin("order-created")
            .where(new SimpleCondition<Event>() {
                @Override
                public boolean filter(Event event) {
                    return event instanceof OrderCreated;
                }
            })
            .notNext("payment-received")
            .where(new SimpleCondition<Event>() {
                @Override
                public boolean filter(Event event) {
                    return event instanceof PaymentReceived;
                }
            })
            .within(Time.minutes(5)) // Timeout after 5 minutes
            .build();
    }
    
    /**
     * Create a pattern for Order Cancellation detection.
     * [semantic-anchor: scenario.order-cancellation]
     */
    private Pattern<Event, ?> createOrderCancellationPattern() {
        return Pattern.<Event>begin("order-cancelled")
            .where(new SimpleCondition<Event>() {
                @Override
                public boolean filter(Event event) {
                    return event instanceof OrderCancelled;
                }
            })
            .build();
    }
    
    /**
     * Create a pattern for Shipment Start detection.
     * [semantic-anchor: scenario.shipment-start]
     */
    private Pattern<Event, ?> createShipmentStartPattern() {
        return Pattern.<Event>begin("payment-received")
            .where(new SimpleCondition<Event>() {
                @Override
                public boolean filter(Event event) {
                    return event instanceof PaymentReceived;
                }
            })
            .next("shipment-started")
            .where(new SimpleCondition<Event>() {
                @Override
                public boolean filter(Event event) {
                    return event instanceof ShipmentStarted;
                }
            })
            .within(Time.minutes(30)) // Shipment should start within 30 minutes of payment
            .build();
    }
    
    /**
     * Create a pattern for Complete Order Flow detection.
     */
    private Pattern<Event, ?> createCompleteOrderFlowPattern() {
        return Pattern.<Event>begin("order-created")
            .where(new SimpleCondition<Event>() {
                @Override
                public boolean filter(Event event) {
                    return event instanceof OrderCreated;
                }
            })
            .next("payment-received")
            .where(new SimpleCondition<Event>() {
                @Override
                public boolean filter(Event event) {
                    return event instanceof PaymentReceived;
                }
            })
            .next("shipment-started")
            .where(new SimpleCondition<Event>() {
                @Override
                public boolean filter(Event event) {
                    return event instanceof ShipmentStarted;
                }
            })
            .within(Time.minutes(30)) // Complete flow within 30 minutes
            .build();
    }
    
    /**
     * Create a pattern for Payment Failure with Order Cancellation.
     */
    private Pattern<Event, ?> createPaymentFailurePattern() {
        return Pattern.<Event>begin("order-created")
            .where(new SimpleCondition<Event>() {
                @Override
                public boolean filter(Event event) {
                    return event instanceof OrderCreated;
                }
            })
            .next("payment-failed")
            .where(new SimpleCondition<Event>() {
                @Override
                public boolean filter(Event event) {
                    return event instanceof PaymentFailed;
                }
            })
            .next("order-cancelled")
            .where(new SimpleCondition<Event>() {
                @Override
                public boolean filter(Event event) {
                    return event instanceof OrderCancelled;
                }
            })
            .within(Time.minutes(15)) // Payment failure and cancellation within 15 minutes
            .build();
    }
    
    /**
     * Create a condition that checks if events belong to the same order.
     */
    private static class SameOrderCondition extends IterativeCondition<Event> {
        @Override
        public boolean filter(Event event, Context<Event> context) {
            if (context.getEvents().isEmpty()) {
                return true; // First event always matches
            }
            
            // Get the order ID from the first event in the pattern
            Event firstEvent = context.getEvents().iterator().next();
            String firstOrderId = getOrderId(firstEvent);
            String currentOrderId = getOrderId(event);
            
            return firstOrderId.equals(currentOrderId);
        }
        
        private String getOrderId(Event event) {
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
            }
            return "";
        }
    }
    
    /**
     * Build the Flink CEP job.
     * @return the configured StreamExecutionEnvironment
     */
    public StreamExecutionEnvironment buildJob() {
        // Set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Configure parallelism for learning (keep it simple)
        env.setParallelism(1);
        
        // Create Kafka source
        KafkaSource<String> kafkaSource = createKafkaSource();
        
        // Create the event stream
        DataStream<String> kafkaStream = env.fromSource(
            kafkaSource, 
            WatermarkStrategy.noWatermarks(),
            "Kafka Source"
        );
        
        // Parse JSON to Event objects [semantic-anchor: decision.json-serialization]
        DataStream<Event> eventStream = kafkaStream
            .map(new EventDeserializer())
            .name("JSON to Event Parser");
        
        // Log all incoming events
        eventStream.process(new org.apache.flink.streaming.api.functions.ProcessFunction<Event, Event>() {
            @Override
            public void processElement(Event event, Context ctx, Collector<Event> out) {
                logger.debug("FLINK-CEP: Received event - Type: {}, ID: {}", 
                    event.getEventType(), event.getEventId());
                out.collect(event);
            }
        }).name("Event Logger");
        
        // Key by order ID for pattern detection
        KeyedStream<Event, String> keyedEventStream = eventStream
            .keyBy(new OrderIdExtractor());
        
        // Create CEP pattern streams
        
        // 1. Order Creation Pattern [semantic-anchor: scenario.order-creation]
        PatternStream<Event> orderCreationPatternStream = CEP.pattern(
            keyedEventStream,
            createOrderCreationPattern()
        );
        
        DataStream<String> orderCreationResults = orderCreationPatternStream
            .process(new PatternProcessFunction<Event, String>() {
                @Override
                public void processMatch(
                    Map<String, List<Event>> match, 
                    Context ctx, 
                    Collector<String> out) {
                    List<Event> events = match.get("order-created");
                    if (events != null && !events.isEmpty()) {
                        Event event = events.get(0);
                        if (event instanceof OrderCreated) {
                            OrderCreated order = (OrderCreated) event;
                            String result = String.format("OrderCreated:%s:%s", 
                                order.getOrderId(), order.getCustomerId());
                            logger.info("FLINK-CEP: Order Created - Order ID: {}, Customer: {}, Amount: {}",
                                order.getOrderId(), order.getCustomerId(), order.getTotalAmount());
                            out.collect(result);
                        }
                    }
                }
            })
            .name("Order Creation Pattern");
        
        orderCreationResults.print().name("Order Creation Output");
        
        // 2. Payment Processing Pattern [semantic-anchor: scenario.payment-processing]
        PatternStream<Event> paymentProcessingPatternStream = CEP.pattern(
            keyedEventStream,
            createPaymentProcessingPattern()
        );
        
        DataStream<String> paymentProcessingResults = paymentProcessingPatternStream
            .process(new PatternProcessFunction<Event, String>() {
                @Override
                public void processMatch(
                    Map<String, List<Event>> match, 
                    Context ctx, 
                    Collector<String> out) {
                    List<Event> orders = match.get("order-created");
                    List<Event> payments = match.get("payment-received");
                    
                    if (orders != null && !orders.isEmpty() && payments != null && !payments.isEmpty()) {
                        OrderCreated order = (OrderCreated) orders.get(0);
                        PaymentReceived payment = (PaymentReceived) payments.get(0);
                        
                        String result = String.format("PaymentProcessed:%s:%s", 
                            order.getOrderId(), payment.getPaymentId());
                        
                        logger.info("FLINK-CEP: Payment Processing - Order ID: {}, Payment ID: {}, Amount: {}",
                            order.getOrderId(), payment.getPaymentId(), payment.getAmount());
                        out.collect(result);
                    }
                }
            })
            .name("Payment Processing Pattern");
        
        paymentProcessingResults.print().name("Payment Processing Output");
        
        // 3. Payment Timeout Pattern [semantic-anchor: scenario.payment-timeout]
        PatternStream<Event> paymentTimeoutPatternStream = CEP.pattern(
            keyedEventStream,
            createPaymentTimeoutPattern()
        );
        
        DataStream<String> paymentTimeoutResults = paymentTimeoutPatternStream
            .process(new PatternProcessFunction<Event, String>() {
                @Override
                public void processMatch(
                    Map<String, List<Event>> match, 
                    Context ctx, 
                    Collector<String> out) {
                    List<Event> orders = match.get("order-created");
                    if (orders != null && !orders.isEmpty()) {
                        OrderCreated order = (OrderCreated) orders.get(0);
                        String result = String.format("PaymentTimeout:%s", order.getOrderId());
                        
                        logger.warn("FLINK-CEP: Payment Timeout - Order ID: {} has not received payment within 5 minutes",
                            order.getOrderId());
                        out.collect(result);
                    }
                }
            })
            .name("Payment Timeout Pattern");
        
        paymentTimeoutResults.print().name("Payment Timeout Output");
        
        // 4. Order Cancellation Pattern [semantic-anchor: scenario.order-cancellation]
        PatternStream<Event> orderCancellationPatternStream = CEP.pattern(
            keyedEventStream,
            createOrderCancellationPattern()
        );
        
        DataStream<String> orderCancellationResults = orderCancellationPatternStream
            .process(new PatternProcessFunction<Event, String>() {
                @Override
                public void processMatch(
                    Map<String, List<Event>> match, 
                    Context ctx, 
                    Collector<String> out) {
                    List<Event> orders = match.get("order-cancelled");
                    if (orders != null && !orders.isEmpty()) {
                        OrderCancelled order = (OrderCancelled) orders.get(0);
                        String result = String.format("OrderCancelled:%s:%s", 
                            order.getOrderId(), order.getCancellationReason());
                        
                        logger.warn("FLINK-CEP: Order Cancellation - Order ID: {}, Reason: {}",
                            order.getOrderId(), order.getCancellationReason());
                        out.collect(result);
                    }
                }
            })
            .name("Order Cancellation Pattern");
        
        orderCancellationResults.print().name("Order Cancellation Output");
        
        // 5. Shipment Start Pattern [semantic-anchor: scenario.shipment-start]
        PatternStream<Event> shipmentStartPatternStream = CEP.pattern(
            keyedEventStream,
            createShipmentStartPattern()
        );
        
        DataStream<String> shipmentStartResults = shipmentStartPatternStream
            .process(new PatternProcessFunction<Event, String>() {
                @Override
                public void processMatch(
                    Map<String, List<Event>> match, 
                    Context ctx, 
                    Collector<String> out) {
                    List<Event> payments = match.get("payment-received");
                    List<Event> shipments = match.get("shipment-started");
                    
                    if (payments != null && !payments.isEmpty() && shipments != null && !shipments.isEmpty()) {
                        PaymentReceived payment = (PaymentReceived) payments.get(0);
                        ShipmentStarted shipment = (ShipmentStarted) shipments.get(0);
                        
                        String result = String.format("ShipmentStarted:%s:%s", 
                            payment.getOrderId(), shipment.getShipmentId());
                        
                        logger.info("FLINK-CEP: Shipment Start - Order ID: {}, Shipment ID: {}, Tracking: {}",
                            payment.getOrderId(), shipment.getShipmentId(), shipment.getTrackingNumber());
                        out.collect(result);
                    }
                }
            })
            .name("Shipment Start Pattern");
        
        shipmentStartResults.print().name("Shipment Start Output");
        
        // 6. Complete Order Flow Pattern
        PatternStream<Event> completeOrderFlowPatternStream = CEP.pattern(
            keyedEventStream,
            createCompleteOrderFlowPattern()
        );
        
        DataStream<String> completeOrderFlowResults = completeOrderFlowPatternStream
            .process(new PatternProcessFunction<Event, String>() {
                @Override
                public void processMatch(
                    Map<String, List<Event>> match, 
                    Context ctx, 
                    Collector<String> out) {
                    List<Event> orders = match.get("order-created");
                    List<Event> payments = match.get("payment-received");
                    List<Event> shipments = match.get("shipment-started");
                    
                    if (orders != null && !orders.isEmpty() && 
                        payments != null && !payments.isEmpty() && 
                        shipments != null && !shipments.isEmpty()) {
                        OrderCreated order = (OrderCreated) orders.get(0);
                        PaymentReceived payment = (PaymentReceived) payments.get(0);
                        ShipmentStarted shipment = (ShipmentStarted) shipments.get(0);
                        
                        String result = String.format("CompleteFlow:%s", order.getOrderId());
                        
                        logger.info("FLINK-CEP: Complete Order Flow - Order ID: {} processed from creation to shipment",
                            order.getOrderId());
                        out.collect(result);
                    }
                }
            })
            .name("Complete Order Flow Pattern");
        
        completeOrderFlowResults.print().name("Complete Flow Output");
        
        // 7. Payment Failure Pattern
        PatternStream<Event> paymentFailurePatternStream = CEP.pattern(
            keyedEventStream,
            createPaymentFailurePattern()
        );
        
        DataStream<String> paymentFailureResults = paymentFailurePatternStream
            .process(new PatternProcessFunction<Event, String>() {
                @Override
                public void processMatch(
                    Map<String, List<Event>> match, 
                    Context ctx, 
                    Collector<String> out) {
                    List<Event> orders = match.get("order-created");
                    List<Event> payments = match.get("payment-failed");
                    List<Event> cancellations = match.get("order-cancelled");
                    
                    if (orders != null && !orders.isEmpty() && 
                        payments != null && !payments.isEmpty() && 
                        cancellations != null && !cancellations.isEmpty()) {
                        OrderCreated order = (OrderCreated) orders.get(0);
                        PaymentFailed payment = (PaymentFailed) payments.get(0);
                        OrderCancelled cancellation = (OrderCancelled) cancellations.get(0);
                        
                        String result = String.format("PaymentFailureFlow:%s:%s", 
                            order.getOrderId(), payment.getFailureReason());
                        
                        logger.warn("FLINK-CEP: Payment Failure Flow - Order ID: {}, Reason: {}, Code: {}",
                            order.getOrderId(), payment.getFailureReason(), payment.getFailureCode());
                        out.collect(result);
                    }
                }
            })
            .name("Payment Failure Pattern");
        
        paymentFailureResults.print().name("Payment Failure Output");
        
        return env;
    }
    
    /**
     * Start the Flink CEP job.
     */
    public void start() {
        try {
            logger.info("Starting Flink CEP Processor...");
            logger.info("Bootstrap Servers: {}", bootstrapServers);
            logger.info("Topic: {}", topic);
            logger.info("Group ID: {}", groupId);
            
            // Build the job
            StreamExecutionEnvironment env = buildJob();
            
            // Execute the job
            env.execute("Event Processing Lab - Flink CEP");
            
        } catch (Exception e) {
            logger.error("Error starting Flink CEP job: " + e.getMessage(), e);
            throw e;
        }
    }
    
    /**
     * Main method to run the Flink CEP Processor from command line.
     * @param args command line arguments: [bootstrap-servers] [topic] [group-id]
     */
    public static void main(String[] args) {
        String bootstrapServers = "localhost:9092";
        String topic = "order-events";
        String groupId = "flink-cep-group";
        
        if (args.length > 0) {
            bootstrapServers = args[0];
        }
        if (args.length > 1) {
            topic = args[1];
        }
        if (args.length > 2) {
            groupId = args[2];
        }
        
        logger.info("Starting Flink CEP Processor");
        logger.info("Bootstrap Servers: {}", bootstrapServers);
        logger.info("Topic: {}", topic);
        logger.info("Group ID: {}", groupId);
        
        FlinkCepProcessor processor = new FlinkCepProcessor(bootstrapServers, topic, groupId);
        
        // Start processing
        processor.start();
    }
}