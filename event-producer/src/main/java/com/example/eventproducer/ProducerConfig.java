package com.example.eventproducer;

/**
 * Configuration class for the Event Producer.
 * Provides default configuration values and allows customization.
 * [semantic-anchor: component.event-producer]
 */
public class ProducerConfig {
    
    // Default configuration values
    public static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
    public static final String DEFAULT_TOPIC = "order-events";
    public static final int DEFAULT_EVENTS_PER_SECOND = 10;
    public static final String DEFAULT_CLIENT_ID = "event-producer";
    
    // Kafka producer configuration
    private String bootstrapServers = DEFAULT_BOOTSTRAP_SERVERS;
    private String topic = DEFAULT_TOPIC;
    private String clientId = DEFAULT_CLIENT_ID;
    
    // Producer behavior configuration
    private int eventsPerSecond = DEFAULT_EVENTS_PER_SECOND;
    private boolean autoFlush = true;
    
    // Kafka producer settings
    private String acks = "all";
    private int retries = 3;
    private int lingerMs = 5;
    private int batchSize = 16384;
    private long bufferMemory = 33554432L;
    private long maxBlockMs = 60000L;
    
    public ProducerConfig() {
    }
    
    public ProducerConfig(String bootstrapServers, String topic) {
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
    }
    
    public String getBootstrapServers() {
        return bootstrapServers;
    }
    
    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }
    
    public String getTopic() {
        return topic;
    }
    
    public void setTopic(String topic) {
        this.topic = topic;
    }
    
    public String getClientId() {
        return clientId;
    }
    
    public void setClientId(String clientId) {
        this.clientId = clientId;
    }
    
    public int getEventsPerSecond() {
        return eventsPerSecond;
    }
    
    public void setEventsPerSecond(int eventsPerSecond) {
        this.eventsPerSecond = eventsPerSecond;
    }
    
    public boolean isAutoFlush() {
        return autoFlush;
    }
    
    public void setAutoFlush(boolean autoFlush) {
        this.autoFlush = autoFlush;
    }
    
    public String getAcks() {
        return acks;
    }
    
    public void setAcks(String acks) {
        this.acks = acks;
    }
    
    public int getRetries() {
        return retries;
    }
    
    public void setRetries(int retries) {
        this.retries = retries;
    }
    
    public int getLingerMs() {
        return lingerMs;
    }
    
    public void setLingerMs(int lingerMs) {
        this.lingerMs = lingerMs;
    }
    
    public int getBatchSize() {
        return batchSize;
    }
    
    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }
    
    public long getBufferMemory() {
        return bufferMemory;
    }
    
    public void setBufferMemory(long bufferMemory) {
        this.bufferMemory = bufferMemory;
    }
    
    public long getMaxBlockMs() {
        return maxBlockMs;
    }
    
    public void setMaxBlockMs(long maxBlockMs) {
        this.maxBlockMs = maxBlockMs;
    }
    
    /**
     * Create a ProducerConfig from environment variables or system properties.
     * @return a configured ProducerConfig instance
     */
    public static ProducerConfig fromEnvironment() {
        ProducerConfig config = new ProducerConfig();
        
        // Check environment variables
        String bootstrapServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
        if (bootstrapServers != null) {
            config.setBootstrapServers(bootstrapServers);
        }
        
        String topic = System.getenv("KAFKA_TOPIC");
        if (topic != null) {
            config.setTopic(topic);
        }
        
        String rate = System.getenv("EVENT_RATE");
        if (rate != null) {
            config.setEventsPerSecond(Integer.parseInt(rate));
        }
        
        // Check system properties as fallback
        if (config.getBootstrapServers().equals(DEFAULT_BOOTSTRAP_SERVERS)) {
            String propBootstrap = System.getProperty("kafka.bootstrap.servers");
            if (propBootstrap != null) {
                config.setBootstrapServers(propBootstrap);
            }
        }
        
        if (config.getTopic().equals(DEFAULT_TOPIC)) {
            String propTopic = System.getProperty("kafka.topic.orders");
            if (propTopic != null) {
                config.setTopic(propTopic);
            }
        }
        
        return config;
    }
    
    @Override
    public String toString() {
        return String.format(
            "ProducerConfig{bootstrapServers='%s', topic='%s', clientId='%s', " +
            "eventsPerSecond=%d, autoFlush=%s, acks='%s', retries=%d, " +
            "lingerMs=%d, batchSize=%d, bufferMemory=%d, maxBlockMs=%d}",
            bootstrapServers, topic, clientId, eventsPerSecond, autoFlush, 
            acks, retries, lingerMs, batchSize, bufferMemory, maxBlockMs
        );
    }
}