# Event Processing Lab - Project Summary

[semantic-anchor: event-processing-lab]

## Project Overview

The **Event Processing Lab** is a comprehensive Java 21 Gradle multi-module learning project designed to compare Kafka, Esper, Apache Flink, and Flink CEP from the perspective of an integration architect. The project demonstrates different approaches to event processing using a common order lifecycle use case.

## Project Structure

```
event-processing-lab/
├── AGENTS.md                              # Agent instructions and project guidelines
├── README.md                              # Project overview and documentation
├── PROJECT_SUMMARY.md                    # This file
├── settings.gradle                        # Gradle multi-module configuration
├── build.gradle                           # Root build configuration
├── gradle.properties                      # Project properties and configurations
├── docker-compose.kafka.yml              # Kafka Docker Compose configuration
├── .gitignore                            # Git ignore patterns
├── .gitattributes                        # Git attributes
├── gradlew & gradle/                     # Gradle wrapper
│
├── src/
│   └── docs/
│       ├── architecture.adoc              # arc42 architecture documentation
│       └── adr/
│           ├── README.md                  # ADR index
│           ├── ADR-001.md                # Use Kafka as Event Backbone
│           ├── ADR-002.md                # Use JSON Serialization
│           ├── ADR-003.md                # Plain Java over Spring Boot
│           └── ADR-004.md                # Separate Implementations
│
├── event-model/                          # [semantic-anchor: component.event-model]
│   ├── build.gradle
│   └── src/main/java/com/example/eventmodel/
│       ├── Event.java                     # Base event class with JSON annotations
│       ├── EventType.java                 # Event type enumeration
│       ├── EventSerializer.java           # JSON serialization/deserialization
│       ├── EventFactory.java              # Test event generation
│       ├── OrderCreated.java              # Order creation event
│       ├── PaymentReceived.java           # Payment success event
│       ├── PaymentFailed.java             # Payment failure event
│       ├── OrderCancelled.java            # Order cancellation event
│       └── ShipmentStarted.java            # Shipment start event
│
├── event-producer/                       # [semantic-anchor: component.event-producer]
│   ├── build.gradle
│   ├── src/main/java/com/example/eventproducer/
│   │   ├── OrderEventProducer.java        # Main Kafka producer
│   │   └── ProducerConfig.java            # Configuration class
│   └── src/main/resources/
│       └── logback.xml                    # Logging configuration
│
├── kafka-consumer/                       # [semantic-anchor: component.kafka-consumer]
│   ├── build.gradle
│   ├── src/main/java/com/example/kafkaconsumer/
│   │   └── OrderEventConsumer.java        # Basic Kafka consumer
│   └── src/main/resources/
│       └── logback.xml                    # Logging configuration
│
├── esper-cep/                            # [semantic-anchor: component.esper-cep]
│   ├── build.gradle
│   ├── src/main/java/com/example/espercep/
│   │   └── EsperCepEngine.java            # Esper CEP engine with EPL rules
│   └── src/main/resources/
│       └── logback.xml                    # Logging configuration
│
├── flink-basic/                          # [semantic-anchor: component.flink-basic]
│   ├── build.gradle
│   ├── src/main/java/com/example/flinkbasic/
│   │   └── FlinkStreamProcessor.java      # Flink DataStream processing
│   └── src/main/resources/
│       └── logback.xml                    # Logging configuration
│
└── flink-cep/                            # [semantic-anchor: component.flink-cep]
    ├── build.gradle
    ├── src/main/java/com/example/flinkcep/
    │   └── FlinkCepProcessor.java          # Flink CEP pattern detection
    └── src/main/resources/
        └── logback.xml                    # Logging configuration
```

## Technologies Used

| Technology | Version | Purpose | Semantic Anchor |
|------------|---------|---------|-----------------|
| Java | 21 | Core programming language | [semantic-anchor: technology.java] |
| Gradle | 8.5 | Build automation | |
| Apache Kafka | 3.6.1 | Event backbone & transport | [semantic-anchor: technology.kafka] |
| Esper | 8.7.0 | Lightweight CEP engine | [semantic-anchor: technology.esper] |
| Apache Flink | 1.17.1 | Distributed stream processing | [semantic-anchor: technology.flink] |
| Flink CEP | 1.17.1 | Pattern detection library | [semantic-anchor: technology.flink-cep] |
| Jackson | 2.15.3 | JSON serialization | [semantic-anchor: decision.json-serialization] |
| SLF4J + Logback | 2.0.9 + 1.4.14 | Logging framework | |

## Architecture Decisions (ADRs)

### ✅ ADR-001: Use Kafka as Event Backbone [semantic-anchor: decision.kafka-as-event-backbone]
- **Status**: Accepted
- **Decision**: Use Apache Kafka as the central event transport mechanism
- **Rationale**: High throughput, fault tolerance, scalability, excellent ecosystem
- **Alternatives**: RabbitMQ, Pulsar, NATS
- **Pugh Matrix Score**: +29 (highest among alternatives)

### ✅ ADR-002: Use JSON Serialization for First Iteration [semantic-anchor: decision.json-serialization]
- **Status**: Accepted
- **Decision**: Use JSON for event serialization
- **Rationale**: Human-readable, easy to debug, widely supported, good for learning
- **Alternatives**: Avro, Protobuf, Thrift, Java Serialization
- **Pugh Matrix Score**: +29 (highest among alternatives)

### ✅ ADR-003: Keep Spring Boot Optional, Use Plain Java [semantic-anchor: decision.plain-java-over-spring]
- **Status**: Accepted
- **Decision**: Prefer plain Java implementations where simpler
- **Rationale**: Focus on core concepts, reduce dependencies, lower learning barrier
- **Exception**: Spring Boot may be used for complex integration scenarios

### ✅ ADR-004: Separate Esper and Flink Implementations [semantic-anchor: decision.separate-implementations]
- **Status**: Accepted
- **Decision**: Maintain separate implementations for direct comparison
- **Rationale**: Direct comparison, no abstraction leakage, clear technology demonstration
- **Shared**: Event model, common interfaces

## Quality Goals

| Priority | Quality Goal | Description | Implementation |
|----------|--------------|-------------|----------------|
| 1 | Learnability [semantic-anchor: quality.learnability] | Enable quick understanding of each technology | Focused examples, clear documentation |
| 2 | Understandability [semantic-anchor: quality.understandability] | Keep code and architecture simple | Consistent patterns, semantic anchors |
| 3 | Scalability [semantic-anchor: quality.scalability] | Demonstrate scaling characteristics | Each module can scale independently |
| 4 | Modifiability [semantic-anchor: quality.modifiability] | Allow easy experimentation | Loose coupling, clear interfaces |
| 5 | Observability [semantic-anchor: quality.observability] | Include logging and metrics | Comprehensive logging in each module |

## Use Case: Order Lifecycle

The project implements a complete order lifecycle with the following events:

### Event Types

| Event | Description | Trigger | Semantic Anchor |
|-------|-------------|---------|-----------------|
| `OrderCreated` | New order placed | Customer checkout | [semantic-anchor: scenario.order-creation] |
| `PaymentReceived` | Payment successful | Payment gateway | [semantic-anchor: scenario.payment-processing] |
| `PaymentFailed` | Payment failed | Payment gateway | [semantic-anchor: scenario.payment-processing] |
| `OrderCancelled` | Order cancelled | Customer/system | [semantic-anchor: scenario.order-cancellation] |
| `ShipmentStarted` | Order shipped | Warehouse | [semantic-anchor: scenario.shipment-start] |

### Runtime Scenarios

All scenarios are implemented across all technologies for direct comparison:

1. **Order Creation Flow** [semantic-anchor: scenario.order-creation]
   - Customer places order → OrderCreated event published → All consumers process

2. **Payment Processing Flow** [semantic-anchor: scenario.payment-processing]
   - OrderCreated → PaymentReceived → Correlate payment with order

3. **Payment Timeout Detection** [semantic-anchor: scenario.payment-timeout]
   - OrderCreated → No PaymentReceived within 5 minutes → Timeout detected

4. **Order Cancellation Flow** [semantic-anchor: scenario.order-cancellation]
   - OrderCreated → OrderCancelled → Process cancellation and refund logic

5. **Shipment Start Flow** [semantic-anchor: scenario.shipment-start]
   - OrderCreated → PaymentReceived → ShipmentStarted → Validate complete flow

## Implementation Details

### Event Model Module [semantic-anchor: component.event-model]

**Purpose**: Common event DTOs and serialization for all modules.

**Key Classes**:
- `Event` - Base class with Jackson polymorphic serialization
- `EventType` - Enumeration of all event types
- `EventSerializer` - JSON serialization/deserialization utilities
- `EventFactory` - Factory for generating test events
- `OrderCreated`, `PaymentReceived`, `PaymentFailed`, `OrderCancelled`, `ShipmentStarted` - Event DTOs

**Features**:
- Polymorphic JSON serialization using Jackson annotations
- Consistent event structure with ID, type, timestamp, version
- Test event generation for development and testing
- Type-safe event handling

### Event Producer Module [semantic-anchor: component.event-producer]

**Purpose**: Generate and publish events to Kafka for processing.

**Key Classes**:
- `OrderEventProducer` - Main Kafka producer with configurable rate
- `ProducerConfig` - Configuration management

**Features**:
- Configurable event generation rate (events per second)
- Random event generation based on realistic probabilities
- Specific scenario generation (complete flows, failure flows)
- Kafka producer with durable configuration
- Graceful startup and shutdown

**Runtime**:
```bash
# Start with default configuration
./gradlew :event-producer:run

# Start with custom configuration
./gradlew :event-producer:run --args="kafka:9092 order-events 20"
```

### Kafka Consumer Module [semantic-anchor: component.kafka-consumer]

**Purpose**: Basic event consumption and processing from Kafka.

**Key Classes**:
- `OrderEventConsumer` - Main Kafka consumer with event processing

**Features**:
- Kafka consumer with manual offset management
- Event type-specific processing methods
- Simple event correlation using in-memory cache
- Graceful shutdown handling
- Comprehensive logging

**Runtime**:
```bash
# Start with default configuration
./gradlew :kafka-consumer:run

# Start with custom configuration
./gradlew :kafka-consumer:run --args="kafka:9092 order-events kafka-consumer-group"
```

### Esper CEP Module [semantic-anchor: component.esper-cep]

**Purpose**: Complex Event Processing using Esper's EPL language.

**Key Classes**:
- `EsperCepEngine` - Main Esper CEP engine with Kafka integration

**Features**:
- Esper engine initialization and configuration
- EPL statement registration for all scenarios:
  - Order creation detection
  - Payment processing pattern
  - Payment timeout detection
  - Order cancellation detection
  - Shipment start detection
  - Complete order flow detection
  - Payment failure pattern detection
  - Order state machine
- Pattern result processing with detailed logging
- Statistics tracking

**EPL Examples**:
```sql
-- Payment Processing Pattern
SELECT oc.orderId, pr.paymentId, pr.amount, pr.paymentMethod 
FROM pattern[every oc=OrderCreated -> pr=PaymentReceived(oc.orderId = pr.orderId)]

-- Payment Timeout Detection
SELECT oc.orderId, oc.timestamp as orderTimestamp 
FROM OrderCreated as oc 
WHERE NOT EXISTS (PaymentReceived(orderId = oc.orderId)) 
AND timer:within(5 min) 
OUTPUT ALL EVERY 1 min

-- Complete Order Flow
SELECT oc.orderId, oc.timestamp as orderTime, 
       pr.paymentId, pr.timestamp as paymentTime, 
       ss.shipmentId, ss.timestamp as shipmentTime 
FROM pattern[every oc=OrderCreated -> pr=PaymentReceived(oc.orderId = pr.orderId) -> 
       ss=ShipmentStarted(pr.orderId = ss.orderId)]
```

**Runtime**:
```bash
# Start with default configuration
./gradlew :esper-cep:run

# Start with custom configuration
./gradlew :esper-cep:run --args="kafka:9092 order-events esper-cep-group"
```

### Flink Basic Module [semantic-anchor: component.flink-basic]

**Purpose**: Stream processing using Apache Flink's DataStream API.

**Key Classes**:
- `FlinkStreamProcessor` - Main Flink streaming job

**Features**:
- Kafka source integration
- Event deserialization and parsing
- Stream splitting by event type
- Event type-specific processing
- Stateful processing with keyed streams
- Pattern detection using RichCoFlatMapFunction
- Window-based processing for timeout detection
- Complete flow detection

**Flink Operations**:
- `map()` - JSON to Event deserialization
- `keyBy()` - Partition by order ID
- `filter()` - Event type filtering
- `connect()` + `flatMap()` - Pattern detection
- `window()` - Time-based windowing
- `process()` - Custom processing logic

**Runtime**:
```bash
# Start with default configuration
./gradlew :flink-basic:run

# Start with custom configuration
./gradlew :flink-basic:run --args="kafka:9092 order-events flink-basic-group"
```

### Flink CEP Module [semantic-anchor: component.flink-cep]

**Purpose**: Pattern detection using Flink's CEP library.

**Key Classes**:
- `FlinkCepProcessor` - Main Flink CEP job

**Features**:
- Kafka source integration
- Event deserialization and parsing
- Pattern definition for all scenarios:
  - Order creation pattern
  - Payment processing pattern
  - Payment timeout pattern
  - Order cancellation pattern
  - Shipment start pattern
  - Complete order flow pattern
  - Payment failure pattern
- Pattern matching with time constraints
- Custom conditions for same-order matching
- Detailed logging of pattern matches

**Pattern Examples**:
```java
// Payment Processing Pattern
Pattern.<Event>begin("order-created")
    .where(event -> event instanceof OrderCreated)
    .next("payment-received")
    .where(event -> event instanceof PaymentReceived)
    .within(Time.minutes(10))
    .build();

// Payment Timeout Pattern
Pattern.<Event>begin("order-created")
    .where(event -> event instanceof OrderCreated)
    .notNext("payment-received")
    .where(event -> event instanceof PaymentReceived)
    .within(Time.minutes(5))
    .build();

// Complete Order Flow Pattern
Pattern.<Event>begin("order-created")
    .where(event -> event instanceof OrderCreated)
    .next("payment-received")
    .where(event -> event instanceof PaymentReceived)
    .next("shipment-started")
    .where(event -> event instanceof ShipmentStarted)
    .within(Time.minutes(30))
    .build();
```

**Runtime**:
```bash
# Start with default configuration
./gradlew :flink-cep:run

# Start with custom configuration
./gradlew :flink-cep:run --args="kafka:9092 order-events flink-cep-group"
```

## Documentation

### Architecture Documentation
- `src/docs/architecture.adoc` - Complete arc42 architecture documentation
  - System context and scope
  - Building block view (Level 1 and 2)
  - Runtime view with sequence diagrams
  - Deployment view
  - Cross-cutting concepts (threat model, security, test, observability, error handling)
  - Architecture decisions (ADR index)
  - Quality scenarios
  - Risks and technical debt
  - Glossary
  - Semantic anchors index

### ADRs (Architecture Decision Records)
- `src/docs/adr/README.md` - ADR index and workflow
- `src/docs/adr/ADR-001.md` - Use Kafka as Event Backbone
- `src/docs/adr/ADR-002.md` - Use JSON Serialization for First Iteration
- `src/docs/adr/ADR-003.md` - Keep Spring Boot Optional, Use Plain Java
- `src/docs/adr/ADR-004.md` - Separate Esper and Flink Implementations

### Semantic Anchors
The project uses semantic anchors extensively for cross-referencing:

- `[semantic-anchor: event-processing-lab]` - Project root
- `[semantic-anchor: component.*]` - Component identifiers
- `[semantic-anchor: technology.*]` - Technology identifiers
- `[semantic-anchor: decision.*]` - Architecture decisions
- `[semantic-anchor: quality.*]` - Quality attributes
- `[semantic-anchor: scenario.*]` - Runtime scenarios
- `[semantic-anchor: risk.*]` - Risks

## Comparison Matrix

| Aspect | Kafka + Consumer | Kafka + Esper | Kafka + Flink | Kafka + Flink CEP |
|--------|------------------|----------------|----------------|-------------------|
| **Scalability** | Medium | Low | High | High |
| **Operational Complexity** | Low | Low | High | High |
| **State Handling** | Manual | Automatic | Automatic | Automatic |
| **Rule/Pattern Complexity** | High | Low | Medium | Medium |
| **Pattern Detection** | Manual | Excellent | Good | Excellent |
| **Latency** | Low | Medium | Medium | Medium |
| **Throughput** | High | Medium | High | High |
| **Learning Curve** | Low | Medium | High | High |
| **Setup Complexity** | Low | Low | High | High |
| **Debugging** | Manual | Good | Good | Good |

## Risks and Mitigations

| Risk | Probability | Impact | Priority | Mitigation | Status |
|------|-------------|--------|----------|------------|--------|
| State explosion in CEP | Medium | High | High | Limit window sizes, use efficient patterns | ✅ Implemented |
| Operational complexity too high | Medium | Medium | Medium | Keep implementations minimal, document thoroughly | ✅ Implemented |
| Technology compatibility issues | Low | Medium | Medium | Use stable versions, test integration early | ✅ Planned |
| Event loss during processing | Low | High | Medium | Use durable commits, monitor offsets | ✅ Implemented |
| Performance bottleneck from JSON | Medium | Medium | Medium | Use efficient libraries, benchmark | ⚠️ Monitor |

## Setup and Running

### Prerequisites
- Java 21 JDK
- Gradle 8.5 (included via wrapper)
- Docker and Docker Compose (for Kafka)

### Quick Start

1. **Start Kafka**:
```bash
# Using Docker Compose
docker-compose -f docker-compose.kafka.yml up -d

# Or manually
# Start Kafka locally and configure in gradle.properties
```

2. **Build the project**:
```bash
./gradlew build
```

3. **Start components** (in separate terminals):
```bash
# Start Event Producer
./gradlew :event-producer:run

# Start Kafka Consumer
./gradlew :kafka-consumer:run

# Start Esper CEP Engine
./gradlew :esper-cep:run

# Start Flink Basic Processor
./gradlew :flink-basic:run

# Start Flink CEP Processor
./gradlew :flink-cep:run
```

### Individual Module Testing

```bash
# Build specific module
./gradlew :event-model:build
./gradlew :event-producer:build

# Run tests for specific module
./gradlew :event-model:test
./gradlew :event-producer:test
```

## Project Statistics

- **Modules**: 6
- **Java Classes**: 30+
- **Lines of Java Code**: ~5,000+
- **Lines of Documentation**: ~10,000+
- **ADRs**: 4
- **Runtime Scenarios**: 5
- **Quality Scenarios**: 18
- **Identified Risks**: 5
- **Technical Debt Items**: 4

## Learning Outcomes

This project enables learners to:

1. **Understand Kafka**: Learn Kafka's role as an event backbone, consumer groups, partitioning, and offset management
2. **Experience Esper**: Understand EPL (Event Processing Language), rule definition, and pattern detection
3. **Explore Flink**: Learn DataStream API, state management, windowing, and watermarks
4. **Discover Flink CEP**: Understand pattern definition, time constraints, and pattern matching
5. **Compare Technologies**: Evaluate trade-offs in scalability, complexity, state handling, and operational requirements
6. **Apply Best Practices**: Learn about semantic anchors, ADRs, arc42 documentation, and quality scenarios

## Comparison Focus Areas

When comparing the technologies, focus on:

### 1. Rule/Pattern Definition
- **Esper**: EPL (Event Processing Language) - declarative SQL-like syntax
- **Flink**: DataStream API - functional, imperative approach
- **Flink CEP**: Pattern API - fluent builder pattern

### 2. State Management
- **Kafka Consumer**: Manual state management with caches
- **Esper**: Automatic state management, windowing, timers
- **Flink**: Manual state, checkpointing, savepoints, windowing
- **Flink CEP**: Pattern state management, time windows

### 3. Time Handling
- **Esper**: Event time, processing time, timer events
- **Flink**: Event time, processing time, watermarks
- **Flink CEP**: Time windows, timeouts in patterns

### 4. Performance Characteristics
- **Throughput**: Events processed per second under load
- **Latency**: Time from event publication to processing completion
- **Memory Usage**: State size and memory consumption
- **CPU Utilization**: Processing overhead

### 5. Operational Complexity
- **Deployment**: Requirements and complexity
- **Configuration**: Number and complexity of configuration options
- **Monitoring**: Available metrics and monitoring capabilities
- **Recovery**: Failure recovery mechanisms and speed

### 6. Development Experience
- **Code Clarity**: Readability and understandability
- **Debugging**: Available tools and techniques
- **Testing**: Approaches to testing
- **Error Handling**: Error detection and recovery

## Future Enhancements

The following enhancements could be made to extend the project:

1. **Additional Event Types**: Add more event types (e.g., InventoryUpdated, CustomerRegistered)
2. **More Complex Patterns**: Implement sophisticated CEP patterns (e.g., fraud detection)
3. **Performance Benchmarks**: Add benchmarking for throughput and latency comparison
4. **Spring Boot Integration**: Add optional Spring Boot implementations
5. **Binary Serialization**: Implement Avro or Protobuf serialization for comparison
6. **Monitoring Integration**: Add Prometheus metrics and Grafana dashboards
7. **Containerization**: Add Docker images for each module
8. **Kubernetes Deployment**: Add Kubernetes manifests for production deployment
9. **Additional Technologies**: Add Apache Spark, Pulsar Functions, or AWS Kinesis
10. **Interactive Tutorials**: Add step-by-step guides for each technology

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make changes following the project conventions
4. Update documentation as needed
5. Ensure all tests pass
6. Submit a pull request

### Commit Message Format
Use Conventional Commits:
```
feat: add new event type
fix: correct payment timeout logic
docs: update architecture documentation
chore: update dependencies
```

## License
MIT License - see LICENSE file for details.

## Documentation References

- [Project README](README.md)
- [Agent Instructions](AGENTS.md)
- [arc42 Architecture](src/docs/architecture.adoc)
- [ADR Index](src/docs/adr/README.md)
- [Kafka Documentation](https://kafka.apache.org/documentation/)
- [Esper Documentation](https://www.espertech.com/esper/documentation.html)
- [Apache Flink Documentation](https://nightlies.apache.org/flink/flink-docs-stable/)
- [Flink CEP Documentation](https://nightlies.apache.org/flink/flink-docs-stable/docs/libs/cep/)

## Project Metadata

- **Project Name**: Event Processing Lab
- **Semantic Anchor**: [semantic-anchor: event-processing-lab]
- **Version**: 1.0.0-SNAPSHOT
- **Java Version**: 21
- **Gradle Version**: 8.5
- **Last Updated**: 2026-07-02
- **Status**: Active Development
- **Maintainer**: Integration Architecture Team

---

*This project was created following the docs-as-code-toolkit/architecture-knowledge-toolkit guidelines.*
