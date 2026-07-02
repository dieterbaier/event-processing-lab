package com.example.eventmodel;

/**
 * Enumeration of all event types in the order lifecycle.
 * [semantic-anchor: component.event-model]
 */
public enum EventType {
    
    /**
     * Event emitted when a new order is created in the system.
     * [semantic-anchor: scenario.order-creation]
     */
    ORDER_CREATED("OrderCreated"),
    
    /**
     * Event emitted when payment for an order is successfully received.
     * [semantic-anchor: scenario.payment-processing]
     */
    PAYMENT_RECEIVED("PaymentReceived"),
    
    /**
     * Event emitted when payment for an order fails.
     * [semantic-anchor: scenario.payment-processing]
     */
    PAYMENT_FAILED("PaymentFailed"),
    
    /**
     * Event emitted when an order is cancelled by customer or system.
     * [semantic-anchor: scenario.order-cancellation]
     */
    ORDER_CANCELLED("OrderCancelled"),
    
    /**
     * Event emitted when shipment for an order starts.
     * [semantic-anchor: scenario.shipment-start]
     */
    SHIPMENT_STARTED("ShipmentStarted");
    
    private final String typeName;
    
    EventType(String typeName) {
        this.typeName = typeName;
    }
    
    public String getTypeName() {
        return typeName;
    }
    
    /**
     * Get EventType from string representation.
     * @param typeName the string representation
     * @return the corresponding EventType
     */
    public static EventType fromString(String typeName) {
        for (EventType type : values()) {
            if (type.typeName.equalsIgnoreCase(typeName)) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown event type: " + typeName);
    }
    
    @Override
    public String toString() {
        return typeName;
    }
}