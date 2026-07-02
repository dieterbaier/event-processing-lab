package com.example.eventmodel;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;

/**
 * Utility class for serializing and deserializing events to/from JSON.
 * [semantic-anchor: component.event-model]
 * [semantic-anchor: decision.json-serialization]
 */
public class EventSerializer {
    
    private static final ObjectMapper objectMapper = createObjectMapper();
    
    private static ObjectMapper createObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        // Register JavaTimeModule for Instant serialization
        mapper.registerModule(new JavaTimeModule());
        // Disable writing dates as timestamps, use ISO-8601 format
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        // Enable pretty printing for development/debugging
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
        return mapper;
    }
    
    /**
     * Serialize an event to JSON string.
     * @param event the event to serialize
     * @return JSON string representation of the event
     * @throws EventSerializationException if serialization fails
     */
    public static String serialize(Event event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new EventSerializationException("Failed to serialize event: " + event, e);
        }
    }
    
    /**
     * Serialize an event to JSON byte array.
     * @param event the event to serialize
     * @return JSON byte array representation of the event
     * @throws EventSerializationException if serialization fails
     */
    public static byte[] serializeToBytes(Event event) {
        try {
            return objectMapper.writeValueAsBytes(event);
        } catch (JsonProcessingException e) {
            throw new EventSerializationException("Failed to serialize event to bytes: " + event, e);
        }
    }
    
    /**
     * Deserialize JSON string to an Event object.
     * Uses polymorphic deserialization based on the eventType field.
     * @param json the JSON string to deserialize
     * @return the deserialized Event object
     * @throws EventDeserializationException if deserialization fails
     */
    public static Event deserialize(String json) {
        try {
            return objectMapper.readValue(json, Event.class);
        } catch (IOException e) {
            throw new EventDeserializationException("Failed to deserialize event from JSON: " + json, e);
        }
    }
    
    /**
     * Deserialize JSON byte array to an Event object.
     * @param bytes the JSON byte array to deserialize
     * @return the deserialized Event object
     * @throws EventDeserializationException if deserialization fails
     */
    public static Event deserializeFromBytes(byte[] bytes) {
        try {
            return objectMapper.readValue(bytes, Event.class);
        } catch (IOException e) {
            throw new EventDeserializationException("Failed to deserialize event from bytes", e);
        }
    }
    
    /**
     * Deserialize JSON string to a specific event type.
     * @param json the JSON string to deserialize
     * @param eventClass the specific event class to deserialize to
     * @param <T> the event type
     * @return the deserialized event object
     * @throws EventDeserializationException if deserialization fails
     */
    public static <T extends Event> T deserializeToType(String json, Class<T> eventClass) {
        try {
            return objectMapper.readValue(json, eventClass);
        } catch (IOException e) {
            throw new EventDeserializationException(
                "Failed to deserialize event to type " + eventClass.getSimpleName(), e);
        }
    }
    
    /**
     * Get the underlying ObjectMapper for advanced use cases.
     * @return the configured ObjectMapper
     */
    public static ObjectMapper getObjectMapper() {
        return objectMapper;
    }
    
    /**
     * Custom exception for event serialization errors.
     */
    public static class EventSerializationException extends RuntimeException {
        public EventSerializationException(String message) {
            super(message);
        }
        
        public EventSerializationException(String message, Throwable cause) {
            super(message, cause);
        }
    }
    
    /**
     * Custom exception for event deserialization errors.
     */
    public static class EventDeserializationException extends RuntimeException {
        public EventDeserializationException(String message) {
            super(message);
        }
        
        public EventDeserializationException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}