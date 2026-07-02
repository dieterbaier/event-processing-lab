package com.example.eventmodel;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.time.Instant;
import java.util.UUID;

/**
 * Base class for all events in the Event Processing Lab.
 * Uses Jackson annotations for polymorphic JSON serialization/deserialization.
 * [semantic-anchor: component.event-model]
 * [semantic-anchor: decision.json-serialization]
 */
@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    property = "eventType"
)
@JsonSubTypes({
    @JsonSubTypes.Type(value = OrderCreated.class, name = "OrderCreated"),
    @JsonSubTypes.Type(value = PaymentReceived.class, name = "PaymentReceived"),
    @JsonSubTypes.Type(value = PaymentFailed.class, name = "PaymentFailed"),
    @JsonSubTypes.Type(value = OrderCancelled.class, name = "OrderCancelled"),
    @JsonSubTypes.Type(value = ShipmentStarted.class, name = "ShipmentStarted")
})
public abstract class Event {
    
    /**
     * Unique identifier for this event.
     */
    private String eventId;
    
    /**
     * Type of the event (discriminator for polymorphic deserialization).
     */
    private String eventType;
    
    /**
     * Timestamp when the event occurred.
     */
    private Instant timestamp;
    
    /**
     * Version of the event schema.
     */
    private String version = "1.0";
    
    protected Event() {
        // Default constructor for Jackson
    }
    
    protected Event(String eventType) {
        this.eventId = UUID.randomUUID().toString();
        this.eventType = eventType;
        this.timestamp = Instant.now();
    }
    
    public String getEventId() {
        return eventId;
    }
    
    public void setEventId(String eventId) {
        this.eventId = eventId;
    }
    
    public String getEventType() {
        return eventType;
    }
    
    public void setEventType(String eventType) {
        this.eventType = eventType;
    }
    
    public Instant getTimestamp() {
        return timestamp;
    }
    
    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }
    
    public String getVersion() {
        return version;
    }
    
    public void setVersion(String version) {
        this.version = version;
    }
    
    /**
     * Get the EventType enum corresponding to this event.
     * @return the EventType
     */
    public EventType getEventTypeEnum() {
        return EventType.fromString(eventType);
    }
    
    @Override
    public String toString() {
        return String.format("Event{eventId='%s', eventType='%s', timestamp=%s, version='%s'}", 
            eventId, eventType, timestamp, version);
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Event event = (Event) o;
        return eventId.equals(event.eventId);
    }
    
    @Override
    public int hashCode() {
        return eventId.hashCode();
    }
}