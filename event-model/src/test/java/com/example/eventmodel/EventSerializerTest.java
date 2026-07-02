package com.example.eventmodel;

import org.junit.jupiter.api.Test;
import java.math.BigDecimal;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for EventSerializer class.
 * [semantic-anchor: component.event-model]
 */
class EventSerializerTest {
    
    @Test
    void testSerializeOrderCreated() {
        OrderCreated orderCreated = EventFactory.createOrderCreated("ORD-12345");
        String json = EventSerializer.serialize(orderCreated);
        
        assertNotNull(json);
        assertTrue(json.contains("OrderCreated"));
        assertTrue(json.contains("ORD-12345"));
        assertTrue(json.contains("eventId"));
        assertTrue(json.contains("timestamp"));
        assertTrue(json.contains("version"));
    }
    
    @Test
    void testDeserializeOrderCreated() {
        String json = "{\"eventType\":\"OrderCreated\",\"orderId\":\"ORD-12345\",\"customerId\":\"CUST-001\",\"totalAmount\":100.00,\"currency\":\"USD\"}";
        
        Event event = EventSerializer.deserialize(json);
        
        assertNotNull(event);
        assertInstanceOf(OrderCreated.class, event);
        assertEquals("OrderCreated", event.getEventType());
        assertEquals("ORD-12345", ((OrderCreated) event).getOrderId());
    }
    
    @Test
    void testRoundTripSerialization() {
        PaymentReceived paymentReceived = new PaymentReceived(
            "ORD-12345", 
            "PAY-001", 
            new BigDecimal("99.99"), 
            "USD", 
            "CreditCard", 
            "TXN-001", 
            "Stripe"
        );
        
        String json = EventSerializer.serialize(paymentReceived);
        Event deserialized = EventSerializer.deserialize(json);
        
        assertInstanceOf(PaymentReceived.class, deserialized);
        PaymentReceived deserializedPayment = (PaymentReceived) deserialized;
        
        assertEquals(paymentReceived.getOrderId(), deserializedPayment.getOrderId());
        assertEquals(paymentReceived.getPaymentId(), deserializedPayment.getPaymentId());
        assertEquals(paymentReceived.getAmount(), deserializedPayment.getAmount());
        assertEquals(paymentReceived.getCurrency(), deserializedPayment.getCurrency());
        assertEquals(paymentReceived.getPaymentMethod(), deserializedPayment.getPaymentMethod());
    }
    
    @Test
    void testAllEventTypes() {
        // Test serialization of all event types
        Event[] events = {
            EventFactory.createOrderCreated("ORD-001"),
            EventFactory.createRandomPaymentReceived("ORD-002"),
            EventFactory.createRandomPaymentFailed("ORD-003"),
            EventFactory.createRandomOrderCancelled("ORD-004"),
            EventFactory.createRandomShipmentStarted("ORD-005")
        };
        
        for (Event event : events) {
            String json = EventSerializer.serialize(event);
            assertNotNull(json);
            assertTrue(json.contains(event.getEventType()));
            
            // Test deserialization
            Event deserialized = EventSerializer.deserialize(json);
            assertNotNull(deserialized);
            assertEquals(event.getClass(), deserialized.getClass());
        }
    }
    
    @Test
    void testEventTypeEnum() {
        assertEquals(EventType.ORDER_CREATED, EventType.fromString("OrderCreated"));
        assertEquals(EventType.PAYMENT_RECEIVED, EventType.fromString("PaymentReceived"));
        assertEquals(EventType.PAYMENT_FAILED, EventType.fromString("PaymentFailed"));
        assertEquals(EventType.ORDER_CANCELLED, EventType.fromString("OrderCancelled"));
        assertEquals(EventType.SHIPMENT_STARTED, EventType.fromString("ShipmentStarted"));
        
        // Test case insensitive
        assertEquals(EventType.ORDER_CREATED, EventType.fromString("ordercreated"));
        assertEquals(EventType.ORDER_CREATED, EventType.fromString("ORDERCREATED"));
    }
    
    @Test
    void testEventTypeEnumThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> {
            EventType.fromString("UnknownEvent");
        });
    }
    
    @Test
    void testOrderCreatedWithItems() {
        OrderCreated orderCreated = new OrderCreated();
        orderCreated.setOrderId("ORD-999");
        orderCreated.setCustomerId("CUST-999");
        orderCreated.setCurrency("USD");
        
        List<OrderCreated.OrderItem> items = List.of(
            new OrderCreated.OrderItem("PROD-001", 2, new BigDecimal("19.99")),
            new OrderCreated.OrderItem("PROD-002", 1, new BigDecimal("29.99"))
        );
        orderCreated.setOrderItems(items);
        
        BigDecimal expectedTotal = new BigDecimal("19.99").multiply(new BigDecimal(2))
            .add(new BigDecimal("29.99"));
        orderCreated.setTotalAmount(expectedTotal);
        
        String json = EventSerializer.serialize(orderCreated);
        Event deserialized = EventSerializer.deserialize(json);
        
        assertInstanceOf(OrderCreated.class, deserialized);
        OrderCreated deserializedOrder = (OrderCreated) deserialized;
        
        assertEquals("ORD-999", deserializedOrder.getOrderId());
        assertEquals("CUST-999", deserializedOrder.getCustomerId());
        assertEquals(2, deserializedOrder.getOrderItems().size());
    }
}