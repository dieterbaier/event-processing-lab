package com.example.eventmodel;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/**
 * Factory class for generating test events for development and testing.
 * [semantic-anchor: component.event-model]
 */
public class EventFactory {
    
    private static final Random random = new Random();
    private static final String[] PRODUCTS = {
        "Laptop", "Phone", "Tablet", "Monitor", "Keyboard", "Mouse", "Headphones"
    };
    private static final String[] CUSTOMERS = {
        "Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace"
    };
    private static final String[] PAYMENT_METHODS = {
        "CreditCard", "DebitCard", "PayPal", "BankTransfer", "ApplePay", "GooglePay"
    };
    private static final String[] PAYMENT_GATEWAYS = {
        "Stripe", "PayPal", "Square", "Adyen", "Braintree"
    };
    private static final String[] CARRIERS = {
        "FedEx", "UPS", "DHL", "USPS", "Amazon Logistics"
    };
    private static final String[] CARRIER_SERVICES = {
        "Standard", "Express", "Overnight", "International", "Priority"
    };
    private static final String[] FAILURE_REASONS = {
        "Insufficient Funds", "Card Expired", "Card Declined", "Invalid Card Number", 
        "Bank Rejected", "Daily Limit Exceeded", "Fraud Suspicion"
    };
    private static final String[] FAILURE_CODES = {
        "INSUFFICIENT_FUNDS", "CARD_EXPIRED", "CARD_DECLINED", "INVALID_CARD", 
        "BANK_REJECTED", "LIMIT_EXCEEDED", "FRAUD_SUSPICION"
    };
    private static final String[] CANCELLATION_REASONS = {
        "Customer Request", "Payment Failed", "Out of Stock", "Price Changed",
        "Shipping Delay", "Changed Mind", "Duplicate Order"
    };
    private static final String[] CANCELLATION_CODES = {
        "CUSTOMER_REQUEST", "PAYMENT_FAILED", "OUT_OF_STOCK", "PRICE_CHANGED",
        "SHIPPING_DELAY", "CHANGED_MIND", "DUPLICATE_ORDER"
    };
    
    /**
     * Generate a random OrderCreated event.
     * @return a new OrderCreated event with random data
     */
    public static OrderCreated createRandomOrderCreated() {
        String orderId = "ORD-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
        String customerId = "CUST-" + UUID.randomUUID().toString().substring(0, 6).toUpperCase();
        
        int itemCount = random.nextInt(1, 4);
        List<OrderCreated.OrderItem> items = createRandomOrderItems(itemCount);
        BigDecimal totalAmount = items.stream()
            .map(OrderCreated.OrderItem::getTotalPrice)
            .reduce(BigDecimal.ZERO, BigDecimal::add);
        
        return new OrderCreated(
            orderId,
            customerId,
            items,
            totalAmount,
            "USD"
        );
    }
    
    /**
     * Generate OrderCreated for a specific order ID.
     * @param orderId the specific order ID to use
     * @return a new OrderCreated event
     */
    public static OrderCreated createOrderCreated(String orderId) {
        String customerId = "CUST-" + UUID.randomUUID().toString().substring(0, 6).toUpperCase();
        
        int itemCount = random.nextInt(1, 4);
        List<OrderCreated.OrderItem> items = createRandomOrderItems(itemCount);
        BigDecimal totalAmount = items.stream()
            .map(OrderCreated.OrderItem::getTotalPrice)
            .reduce(BigDecimal.ZERO, BigDecimal::add);
        
        return new OrderCreated(
            orderId,
            customerId,
            items,
            totalAmount,
            "USD"
        );
    }
    
    /**
     * Generate a random PaymentReceived event for an existing order.
     * @param orderId the order ID to associate the payment with
     * @return a new PaymentReceived event
     */
    public static PaymentReceived createRandomPaymentReceived(String orderId) {
        BigDecimal amount = new BigDecimal(random.nextInt(50, 500) + "." + random.nextInt(0, 99));
        
        return new PaymentReceived(
            orderId,
            "PAY-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase(),
            amount,
            "USD",
            getRandom(PAYMENT_METHODS),
            "TXN-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase(),
            getRandom(PAYMENT_GATEWAYS)
        );
    }
    
    /**
     * Generate a random PaymentFailed event for an existing order.
     * @param orderId the order ID to associate the payment with
     * @return a new PaymentFailed event
     */
    public static PaymentFailed createRandomPaymentFailed(String orderId) {
        BigDecimal amount = new BigDecimal(random.nextInt(50, 500) + "." + random.nextInt(0, 99));
        
        return new PaymentFailed(
            orderId,
            "PAY-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase(),
            amount,
            "USD",
            getRandom(PAYMENT_METHODS),
            getRandom(FAILURE_REASONS),
            getRandom(FAILURE_CODES),
            getRandom(PAYMENT_GATEWAYS)
        );
    }
    
    /**
     * Generate a random OrderCancelled event.
     * @param orderId the order ID to cancel
     * @return a new OrderCancelled event
     */
    public static OrderCancelled createRandomOrderCancelled(String orderId) {
        String customerId = "CUST-" + UUID.randomUUID().toString().substring(0, 6).toUpperCase();
        
        return new OrderCancelled(
            orderId,
            customerId,
            getRandom(CANCELLATION_REASONS),
            getRandom(CANCELLATION_CODES),
            random.nextBoolean(),
            random.nextBoolean() ? "Customer" : "System"
        );
    }
    
    /**
     * Generate a random ShipmentStarted event for an order.
     * @param orderId the order ID to ship
     * @return a new ShipmentStarted event
     */
    public static ShipmentStarted createRandomShipmentStarted(String orderId) {
        return new ShipmentStarted(
            orderId,
            "SHIP-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase(),
            "TRK" + UUID.randomUUID().toString().substring(0, 12).toUpperCase(),
            getRandom(CARRIERS),
            getRandom(CARRIER_SERVICES),
            "Warehouse, City, Country",
            "Customer Address, City, Country",
            random.nextInt(1, 10)
        );
    }
    
    /**
     * Create a complete order lifecycle event sequence.
     * @return array of events representing a complete order flow
     */
    public static Event[] createOrderLifecycleSequence() {
        String orderId = "ORD-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
        
        return new Event[] {
            createOrderCreated(orderId),
            createRandomPaymentReceived(orderId),
            createRandomShipmentStarted(orderId)
        };
    }
    
    /**
     * Create an order lifecycle with payment failure.
     * @return array of events representing order with failed payment
     */
    public static Event[] createOrderWithFailedPaymentSequence() {
        String orderId = "ORD-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
        
        return new Event[] {
            createOrderCreated(orderId),
            createRandomPaymentFailed(orderId),
            createRandomOrderCancelled(orderId)
        };
    }
    
    /**
     * Create an event with a specific timestamp.
     * @param event the event to modify
     * @param timestamp the timestamp to set
     * @return the event with updated timestamp
     */
    public static Event withTimestamp(Event event, Instant timestamp) {
        event.setTimestamp(timestamp);
        return event;
    }
    
    /**
     * Create an event with a specific event ID.
     * @param event the event to modify
     * @param eventId the event ID to set
     * @return the event with updated event ID
     */
    public static Event withEventId(Event event, String eventId) {
        event.setEventId(eventId);
        return event;
    }
    
    private static List<OrderCreated.OrderItem> createRandomOrderItems(int count) {
        return java.util.stream.IntStream.range(0, count)
            .mapToObj(i -> new OrderCreated.OrderItem(
                "PROD-" + UUID.randomUUID().toString().substring(0, 4).toUpperCase(),
                random.nextInt(1, 5),
                new BigDecimal(random.nextInt(10, 200) + "." + random.nextInt(0, 99))
            ))
            .toList();
    }
    
    private static <T> T getRandom(T[] array) {
        return array[random.nextInt(array.length)];
    }
}