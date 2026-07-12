package com.example.flinkbasic;

import com.example.eventmodel.*;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction.Context;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;

/**
 * Apache Flink stream processor for order lifecycle events.
 * Demonstrates basic stream processing using Flink's DataStream API.
 * [semantic-anchor: component.flink-basic]
 * [semantic-anchor: technology.flink]
 */
public class FlinkStreamProcessor {
    
    private static final Logger logger = LoggerFactory.getLogger(FlinkStreamProcessor.class);
    
    private final String bootstrapServers;
    private final String topic;
    private final String groupId;
    
    /**
     * Create a FlinkStreamProcessor with the specified Kafka configuration.
     * @param bootstrapServers Kafka bootstrap servers
     * @param topic Kafka topic to consume from
     * @param groupId Consumer group ID
     */
    public FlinkStreamProcessor(String bootstrapServers, String topic, String groupId) {
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
        this.groupId = groupId;
    }
    
    /**
     * Create and configure the Kafka source.
     * @return the configured KafkaSource
     */
    private KafkaSource<String> createKafkaSource() {
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
    private static class EventDeserializer implements MapFunction<String, Event> {
        @Override
        public Event map(String value) throws Exception {
            return EventSerializer.deserialize(value);
        }
    }
    
    /**
     * Extract order ID from event.
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
     * Process OrderCreated events.
     * [semantic-anchor: scenario.order-creation]
     */
    private static class OrderCreatedProcessor implements FilterFunction<Event> {
        @Override
        public boolean filter(Event event) throws Exception {
            if (event instanceof OrderCreated) {
                OrderCreated order = (OrderCreated) event;
                logger.info("FLINK: OrderCreated - Order ID: {}, Customer: {}, Amount: {}, Items: {}",
                    order.getOrderId(), order.getCustomerId(), 
                    order.getTotalAmount(), order.getOrderItems().size());
                return true;
            }
            return false;
        }
    }
    
    /**
     * Process PaymentReceived events.
     * [semantic-anchor: scenario.payment-processing]
     */
    private static class PaymentReceivedProcessor implements FilterFunction<Event> {
        @Override
        public boolean filter(Event event) throws Exception {
            if (event instanceof PaymentReceived) {
                PaymentReceived payment = (PaymentReceived) event;
                logger.info("FLINK: PaymentReceived - Order ID: {}, Payment ID: {}, Amount: {}, Method: {}",
                    payment.getOrderId(), payment.getPaymentId(), 
                    payment.getAmount(), payment.getPaymentMethod());
                return true;
            }
            return false;
        }
    }
    
    /**
     * Process PaymentFailed events.
     * [semantic-anchor: scenario.payment-processing]
     */
    private static class PaymentFailedProcessor implements FilterFunction<Event> {
        @Override
        public boolean filter(Event event) throws Exception {
            if (event instanceof PaymentFailed) {
                PaymentFailed payment = (PaymentFailed) event;
                logger.warn("FLINK: PaymentFailed - Order ID: {}, Payment ID: {}, Reason: {}, Code: {}",
                    payment.getOrderId(), payment.getPaymentId(), 
                    payment.getFailureReason(), payment.getFailureCode());
                return true;
            }
            return false;
        }
    }
    
    /**
     * Process OrderCancelled events.
     * [semantic-anchor: scenario.order-cancellation]
     */
    private static class OrderCancelledProcessor implements FilterFunction<Event> {
        @Override
        public boolean filter(Event event) throws Exception {
            if (event instanceof OrderCancelled) {
                OrderCancelled order = (OrderCancelled) event;
                logger.warn("FLINK: OrderCancelled - Order ID: {}, Reason: {}, Refund Eligible: {}",
                    order.getOrderId(), order.getCancellationReason(), 
                    order.isRefundEligible());
                return true;
            }
            return false;
        }
    }
    
    /**
     * Process ShipmentStarted events.
     * [semantic-anchor: scenario.shipment-start]
     */
    private static class ShipmentStartedProcessor implements FilterFunction<Event> {
        @Override
        public boolean filter(Event event) throws Exception {
            if (event instanceof ShipmentStarted) {
                ShipmentStarted shipment = (ShipmentStarted) event;
                logger.info("FLINK: ShipmentStarted - Order ID: {}, Shipment ID: {}, Tracking: {}, Carrier: {}",
                    shipment.getOrderId(), shipment.getShipmentId(), 
                    shipment.getTrackingNumber(), shipment.getCarrier());
                return true;
            }
            return false;
        }
    }
    
    /**
     * Extract order ID and event type for grouping.
     */
    private static class OrderIdAndTypeExtractor implements KeySelector<Event, Tuple2<String, EventType>> {
        @Override
        public Tuple2<String, EventType> getKey(Event event) {
            String orderId = "";
            if (event instanceof OrderCreated) {
                orderId = ((OrderCreated) event).getOrderId();
            } else if (event instanceof PaymentReceived) {
                orderId = ((PaymentReceived) event).getOrderId();
            } else if (event instanceof PaymentFailed) {
                orderId = ((PaymentFailed) event).getOrderId();
            } else if (event instanceof OrderCancelled) {
                orderId = ((OrderCancelled) event).getOrderId();
            } else if (event instanceof ShipmentStarted) {
                orderId = ((ShipmentStarted) event).getOrderId();
            }
            return new Tuple2<>(orderId, event.getEventTypeEnum());
        }
    }
    
    /**
     * Detect payment processing pattern: OrderCreated -> PaymentReceived.
     */
    private static class PaymentProcessingPatternDetector 
        extends RichCoFlatMapFunction<OrderCreated, PaymentReceived, String> {
        
        private ValueState<OrderCreated> orderState;
        
        @Override
        public void open(Configuration parameters) {
            // State descriptor
            ValueStateDescriptor<OrderCreated> descriptor = 
                new ValueStateDescriptor<>("paymentOrderState", OrderCreated.class);
            orderState = getRuntimeContext().getState(descriptor);
        }
        
        @Override
        public void flatMap1(OrderCreated order, Collector<String> collector) throws Exception {
            // Store the order in state
            orderState.update(order);
        }
        
        @Override
        public void flatMap2(PaymentReceived payment, Collector<String> collector) throws Exception {
            OrderCreated order = orderState.value();
            if (order != null && order.getOrderId().equals(payment.getOrderId())) {
                // Detected payment processing pattern
                logger.info("FLINK: Payment Processing Pattern - Order ID: {} received payment: {}",
                    payment.getOrderId(), payment.getPaymentId());
                collector.collect("PaymentProcessed:" + payment.getOrderId());
                orderState.clear();
            }
        }
    }
    
    /**
     * Detect complete order flow: OrderCreated -> PaymentReceived -> ShipmentStarted.
     * Commented out due to ProcessWindowFunction API compatibility issues with Flink 1.14.6
     */
    // private static class CompleteOrderFlowDetector 
    //     implements ProcessWindowFunction<Tuple3<String, EventType, Long>, String, String, TimeWindow> {
    //     
    //     @Override
    //     public void process(String key, Context context, 
    //                       Iterable<Tuple3<String, EventType, Long>> events, 
    //                       Collector<String> collector) {
    //         // Collect all event types for this order
    //         Set<EventType> eventTypes = new HashSet<>();
    //         for (Tuple3<String, EventType, Long> event : events) {
    //             eventTypes.add(event.f1);
    //         }
    //         
    //         // Check for complete flow
    //         if (eventTypes.contains(EventType.ORDER_CREATED) &&
    //             eventTypes.contains(EventType.PAYMENT_RECEIVED) &&
    //             eventTypes.contains(EventType.SHIPMENT_STARTED)) {
    //             logger.info("FLINK: Complete Order Flow - Order ID: {} processed from creation to shipment",
    //                 key);
    //             collector.collect("CompleteFlow:" + key);
    //         }
    //     }
    // }
    
    /**
     * Build the Flink streaming job.
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
        eventStream.process(new ProcessFunction<Event, Event>() {
            @Override
            public void processElement(Event event, Context ctx, Collector<Event> out) {
                logger.debug("FLINK: Received event - Type: {}, ID: {}", 
                    event.getEventType(), event.getEventId());
                out.collect(event);
            }
        }).name("Event Logger");
        
        // Split into different event type streams
        OutputTag<Event> orderCreatedTag = new OutputTag<Event>("order-created") {};
        OutputTag<Event> paymentReceivedTag = new OutputTag<Event>("payment-received") {};
        OutputTag<Event> paymentFailedTag = new OutputTag<Event>("payment-failed") {};
        OutputTag<Event> orderCancelledTag = new OutputTag<Event>("order-cancelled") {};
        OutputTag<Event> shipmentStartedTag = new OutputTag<Event>("shipment-started") {};
        
        SingleOutputStreamOperator<Event> processedStream = eventStream
            .process(new ProcessFunction<Event, Event>() {
                @Override
                public void processElement(Event event, Context ctx, Collector<Event> out) {
                    switch (event.getEventTypeEnum()) {
                        case ORDER_CREATED:
                            ctx.output(orderCreatedTag, event);
                            break;
                        case PAYMENT_RECEIVED:
                            ctx.output(paymentReceivedTag, event);
                            break;
                        case PAYMENT_FAILED:
                            ctx.output(paymentFailedTag, event);
                            break;
                        case ORDER_CANCELLED:
                            ctx.output(orderCancelledTag, event);
                            break;
                        case SHIPMENT_STARTED:
                            ctx.output(shipmentStartedTag, event);
                            break;
                        default:
                            out.collect(event);
                    }
                }
            }).name("Event Type Splitter");
        
        // Get side outputs
        DataStream<Event> orderCreatedStream = processedStream.getSideOutput(orderCreatedTag);
        DataStream<Event> paymentReceivedStream = processedStream.getSideOutput(paymentReceivedTag);
        DataStream<Event> paymentFailedStream = processedStream.getSideOutput(paymentFailedTag);
        DataStream<Event> orderCancelledStream = processedStream.getSideOutput(orderCancelledTag);
        DataStream<Event> shipmentStartedStream = processedStream.getSideOutput(shipmentStartedTag);
        
        // Process each event type [semantic-anchor: scenario.order-creation] [semantic-anchor: scenario.payment-processing] etc.
        orderCreatedStream.filter(new OrderCreatedProcessor()).name("Order Created Processor");
        paymentReceivedStream.filter(new PaymentReceivedProcessor()).name("Payment Received Processor");
        paymentFailedStream.filter(new PaymentFailedProcessor()).name("Payment Failed Processor");
        orderCancelledStream.filter(new OrderCancelledProcessor()).name("Order Cancelled Processor");
        shipmentStartedStream.filter(new ShipmentStartedProcessor()).name("Shipment Started Processor");
        
        // Key by order ID for stateful processing
        KeyedStream<Event, String> keyedByOrderId = eventStream
            .keyBy(new OrderIdExtractor());
        
        // Detect payment processing pattern
        // Note: orderCreatedStream is DataStream<Event>, so keyBy returns KeyedStream<Event, String>
        // Commented out due to type system limitations - would need type-safe filtering
        // DataStream<OrderCreated> orderCreatedKeyed = orderCreatedStream
        //     .keyBy(new KeySelector<OrderCreated, String>() {
        //         @Override
        //         public String getKey(OrderCreated value) {
        //             return value.getOrderId();
        //         }
        //     });
        // 
        // DataStream<PaymentReceived> paymentReceivedKeyed = paymentReceivedStream
        //     .keyBy(new KeySelector<PaymentReceived, String>() {
        //         @Override
        //         public String getKey(PaymentReceived value) {
        //             return value.getOrderId();
        //         }
        //     });
        
        // Connect streams for pattern detection
        // Commented out due to type system issues with side outputs
        // DataStream<String> paymentPatterns = orderCreatedKeyed
        //     .connect(paymentReceivedKeyed)
        //     .flatMap(new PaymentProcessingPatternDetector())
        //     .name("Payment Processing Pattern Detector");
        // 
        // // Print pattern detection results
        // paymentPatterns.print().name("Pattern Detection Output");
        
        // Group by order ID and window for complete flow detection
        DataStream<Tuple2<String, EventType>> orderEvents = eventStream
            .map(new MapFunction<Event, Tuple2<String, EventType>>() {
                @Override
                public Tuple2<String, EventType> map(Event event) {
                    String orderId = "";
                    if (event instanceof OrderCreated) {
                        orderId = ((OrderCreated) event).getOrderId();
                    } else if (event instanceof PaymentReceived) {
                        orderId = ((PaymentReceived) event).getOrderId();
                    } else if (event instanceof PaymentFailed) {
                        orderId = ((PaymentFailed) event).getOrderId();
                    } else if (event instanceof OrderCancelled) {
                        orderId = ((OrderCancelled) event).getOrderId();
                    } else if (event instanceof ShipmentStarted) {
                        orderId = ((ShipmentStarted) event).getOrderId();
                    }
                    return new Tuple2<>(orderId, event.getEventTypeEnum());
                }
            })
            .filter(t -> !t.f0.isEmpty()) // Filter out events without order ID
            .name("Order Event Extractor");
        
        // Key by order ID
        KeyedStream<Tuple2<String, EventType>, String> keyedOrderEvents = 
            orderEvents.keyBy(t -> t.f0);
        
        // Window for complete flow detection (5 minutes)
        DataStream<Tuple3<String, EventType, Long>> windowedEvents = keyedOrderEvents
            .map(new MapFunction<Tuple2<String, EventType>, Tuple3<String, EventType, Long>>() {
                @Override
                public Tuple3<String, EventType, Long> map(Tuple2<String, EventType> value) {
                    return new Tuple3<>(value.f0, value.f1, System.currentTimeMillis());
                }
            })
            .name("Event Windowing");
        
        // Window after keying - commented out as window() requires an aggregation operation
        // DataStream<Tuple3<String, EventType, Long>> windowedEvents2 = windowedEvents
        //     .keyBy(t -> t.f0)
        //     .window(TumblingProcessingTimeWindows.of(Time.minutes(5)))
        //     .name("Windowed Event Stream");
        
        // Detect complete order flows
        // Commented out due to ProcessWindowFunction API compatibility issues and windowing configuration
        // DataStream<String> completeFlows = windowedEvents2
        //     .process(new CompleteOrderFlowDetector())
        //     .name("Complete Order Flow Detector");
        // 
        // // Print complete flow results
        // completeFlows.print().name("Complete Flow Output");
        
        return env;
    }
    
    /**
     * Start the Flink streaming job.
     */
    public void start() throws Exception {
        try {
            logger.info("Starting Flink Stream Processor...");
            logger.info("Bootstrap Servers: {}", bootstrapServers);
            logger.info("Topic: {}", topic);
            logger.info("Group ID: {}", groupId);
            
            // Build the job
            StreamExecutionEnvironment env = buildJob();
            
            // Execute the job
            env.execute("Event Processing Lab - Flink Basic");
            
        } catch (Exception e) {
            logger.error("Error starting Flink job: " + e.getMessage(), e);
            throw e;
        }
    }
    
    /**
     * Main method to run the Flink Stream Processor from command line.
     * @param args command line arguments: [bootstrap-servers] [topic] [group-id]
     */
    public static void main(String[] args) {
        String bootstrapServers = "localhost:9092";
        String topic = "order-events";
        String groupId = "flink-basic-group";
        
        if (args.length > 0) {
            bootstrapServers = args[0];
        }
        if (args.length > 1) {
            topic = args[1];
        }
        if (args.length > 2) {
            groupId = args[2];
        }
        
        logger.info("Starting Flink Stream Processor");
        logger.info("Bootstrap Servers: {}", bootstrapServers);
        logger.info("Topic: {}", topic);
        logger.info("Group ID: {}", groupId);
        
        FlinkStreamProcessor processor = new FlinkStreamProcessor(bootstrapServers, topic, groupId);
        
        // Start processing
        try {
            processor.start();
        } catch (Exception e) {
            logger.error("Flink job failed: " + e.getMessage(), e);
            System.exit(1);
        }
    }
}