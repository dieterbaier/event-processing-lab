# Event Processing Lab

[semantic-anchor: event-processing-lab]

*A hands-on learning project comparing Kafka, Esper, Apache Flink, and Flink CEP for event processing.*

## Overview

The Event Processing Lab is designed for integration architects to gain practical experience with different event processing technologies through a common use case: **order lifecycle event processing**.

By implementing the same business scenarios across multiple technologies, this project enables direct comparison of architectural trade-offs, scalability characteristics, operational complexity, state handling approaches, and rule complexity.

## Learning Objectives

[semantic-anchor: quality.learnability]

- Understand Kafka as an event backbone and transport mechanism
- Experience Esper as a lightweight Complex Event Processing (CEP) engine
- Explore Apache Flink as a distributed stream processing framework
- Compare Flink CEP with Esper for pattern detection
- Evaluate trade-offs: scalability, complexity, state management, and operational overhead

## Use Case: Order Lifecycle

[semantic-anchor: scenario.order-creation]
[semantic-anchor: scenario.payment-processing]
[semantic-anchor: scenario.payment-timeout]
[semantic-anchor: scenario.order-cancellation]
[semantic-anchor: scenario.shipment-start]

The project implements an order processing system with the following events:

| Event | Description | Trigger |
|-------|-------------|---------|
| `OrderCreated` | New order placed in the system | Customer checkout |
| `PaymentReceived` | Payment successfully processed | Payment gateway |
| `PaymentFailed` | Payment processing failed | Payment gateway |
| `OrderCancelled` | Order cancelled by customer or system | Customer action / Timeout |
| `ShipmentStarted` | Order shipped to customer | Warehouse system |

## Project Structure

```
event-processing-lab/
├── AGENTS.md                      # Thin agent contract routing to the toolkit
├── README.md                      # This file
├── build.sh                       # docs-toolbox task runner (validate/generate/build)
├── build.gradle                   # Root build configuration
├── settings.gradle                # Gradle multi-module configuration
├── gradle.properties              # Gradle properties
├── metamodel/                     # Vendored artifact/relation schemas (contracts)
├── templates/                     # Vendored ADR/quality-scenario/risk templates
├── scripts/                       # Vendored validator + agent-adapter generators
├── adapters/                      # Generated agent adapters (codex, vibe, copilot, cursor)
├── .github/
│   ├── copilot-instructions.md    # GitHub Copilot entry point
│   └── workflows/                 # CI (validate + tests) and GitHub Pages publish
├── src/
│   └── docs/
│       ├── doc-001-arc42.adoc     # arc42 entry point (assembled documentation)
│       ├── doc-002-vision-mission.adoc
│       ├── doc-004-roadmap.adoc
│       ├── doc-005-questions-and-answers.adoc
│       └── arc42/                 # arc42 chapter sources (+ per-chapter detail dirs)
│           ├── doc-01000-introduction-and-goals.adoc
│           ├── doc-02000-architecture-constraints.adoc
│           ├── doc-03000-system-scope-and-context.adoc
│           ├── doc-04000-solution-strategy.adoc
│           ├── doc-05000-building-block-view.adoc
│           ├── doc-06000-runtime-view.adoc
│           ├── doc-07000-deployment-view.adoc
│           ├── doc-08000-crosscutting-concepts.adoc
│           ├── doc-09000-architecture-decisions.adoc
│           ├── 09-architecture-decisions/    # adr-001 … adr-004-*.adoc
│           ├── doc-10000-quality-requirements.adoc
│           ├── 10-quality-requirements/      # qs-001 … qs-015-*.adoc
│           ├── doc-11000-risks-and-technical-debt.adoc
│           └── 11-risks-and-technical-debt/  # risk-001 … risk-006, td-005 … td-010
├── event-model/                   # [semantic-anchor: component.event-model]
├── event-producer/                # [semantic-anchor: component.event-producer]
├── kafka-consumer/                # [semantic-anchor: component.kafka-consumer]
├── esper-cep/                     # [semantic-anchor: component.esper-cep]
├── flink-basic/                   # [semantic-anchor: component.flink-basic]
└── flink-cep/                     # [semantic-anchor: component.flink-cep]

Each module holds its Java sources under src/main/java/com/example/<module>/.
Derived documentation output lives under src/docs/**/generated/ and build/ and
is not committed (regenerate with ./build.sh).
```

## Technologies

| Technology | Purpose | Module | Status |
|------------|---------|--------|--------|
| [Kafka](https://kafka.apache.org/) | Event Backbone & Transport | event-producer, kafka-consumer | [semantic-anchor: technology.kafka] |
| [Esper](https://www.espertech.com/esper/) | Lightweight CEP Engine | esper-cep | [semantic-anchor: technology.esper] |
| [Apache Flink](https://flink.apache.org/) | Distributed Stream Processing | flink-basic, flink-cep | [semantic-anchor: technology.flink] |
| [Flink CEP](https://nightlies.apache.org/flink/flink-docs-stable/docs/libs/cep/) | Pattern Detection Library | flink-cep | [semantic-anchor: technology.flink-cep] |

## Architecture Documentation

The complete architecture documentation following the arc42 template and docs-as-code-toolkit conventions is available in the `src/docs/` directory:

* link:src/docs/doc-001-arc42.adoc[arc42 Architecture Documentation] - Main entry point
* link:src/docs/arc42/doc-09000-architecture-decisions.adoc[Architecture Decisions] - ADR index and detailed decision records
* link:src/docs/arc42/doc-10000-quality-requirements.adoc[Quality Requirements] - Quality tree and measurable scenarios
* link:src/docs/arc42/doc-11000-risks-and-technical-debt.adoc[Risks and Technical Debt] - Risk assessment and technical debt tracking

### Validating and building the documentation

The architecture docs are validated, generated, and rendered with the
`docs-as-code-toolkit/architecture-knowledge-toolkit` tooling, run through the
pinned `docs-toolbox` container image via `./build.sh`:

```bash
./build.sh validate    # validate artifact metadata and relations
./build.sh generate    # regenerate derived fragments and indexes
./build.sh build       # render build/architecture/index.html
```

Set `DOCS_TOOLBOX_LOCAL=1` to run against the host toolchain instead. CI
(`.github/workflows/ci.yml`) validates every pull request; on `main`, the
documentation is rendered and published to GitHub Pages
(`.github/workflows/pages.yml`).

## Quality Goals

[semantic-anchor: quality.understandability]
[semantic-anchor: quality.scalability]
[semantic-anchor: quality.modifiability]
[semantic-anchor: quality.observability]

### Primary Quality Attributes (arc42 Chapter 1.2)

1. **Learnability** - Enable quick understanding of each technology's capabilities
2. **Understandability** - Simple, well-documented code and architecture
3. **Scalability** - Demonstrate scaling characteristics of each technology
4. **Modifiability** - Easy to experiment with different configurations
5. **Observability** - Include logging and metrics for understanding behavior

### Quality Scenarios (arc42 Chapter 10)

See link:src/docs/arc42/10-quality-requirements.adoc[Quality Requirements] for detailed quality scenarios including:
- Throughput measurement under load
- Latency measurement for event processing
- Rule modification and deployment time
- System recovery from failures
- Monitoring and debugging capabilities

## Runtime Scenarios

The following runtime scenarios are implemented across all technologies:

### 1. Order Creation Flow [semantic-anchor: scenario.order-creation]
```
OrderCreated -> [Processing] -> Order State Updated
```

### 2. Payment Processing Flow [semantic-anchor: scenario.payment-processing]
```
OrderCreated -> PaymentReceived -> [Validation] -> Order Confirmed
```

### 3. Payment Timeout Detection [semantic-anchor: scenario.payment-timeout]
```
OrderCreated -> [30 minute timeout] -> OrderCancelled
```

### 4. Order Cancellation Flow [semantic-anchor: scenario.order-cancellation]
```
OrderCreated -> OrderCancelled -> [Refund Processing] -> Order Cancelled
```

### 5. Shipment Start Flow [semantic-anchor: scenario.shipment-start]
```
OrderCreated -> PaymentReceived -> ShipmentStarted -> [Tracking] -> Order Shipped
```

## Comparison Matrix

| Aspect | Kafka + Consumer | Kafka + Esper | Kafka + Flink | Kafka + Flink CEP |
|--------|------------------|----------------|----------------|-------------------|
| **Scalability** | Medium | Low | High | High |
| **Operational Complexity** | Low | Low | High | High |
| **State Handling** | Manual | Automatic | Automatic | Automatic |
| **Rule Complexity** | High | Low | Medium | Medium |
| **Pattern Detection** | Manual | Excellent | Good | Excellent |
| **Latency** | Low | Medium | Medium | Medium |
| **Throughput** | High | Medium | High | High |
| **Learning Curve** | Low | Medium | High | High |

## Prerequisites

### Java
- Java 21 or later
- JDK with GraalVM native-image support (optional, for native builds)

### Build Tool
- Gradle 8.5 or later

### Infrastructure
- Apache Kafka (local or remote)
- For Flink modules: Apache Flink 1.17+
- For Esper module: Esper 8.0+

## Setup

### 1. Clone the Repository
```bash
git clone https://github.com/your-org/event-processing-lab.git
cd event-processing-lab
```

### 2. Configure Kafka
Start a local Kafka cluster using Docker:
```bash
docker-compose -f docker-compose.kafka.yml up -d
```

Or install Kafka locally and configure the connection in `gradle.properties`:
```properties
kafka.bootstrap.servers=localhost:9092
```

### 3. Build the Project
```bash
# Build all modules
./gradlew build

# Build specific module
./gradlew :event-model:build
./gradlew :event-producer:build
```

### 4. Run Components

#### Start Event Producer
```bash
./gradlew :event-producer:run
```

#### Start Kafka Consumer
```bash
./gradlew :kafka-consumer:run
```

#### Start Esper CEP Engine
```bash
./gradlew :esper-cep:run
```

#### Start Flink Basic Processor
```bash
./gradlew :flink-basic:run
```

#### Start Flink CEP Processor
```bash
./gradlew :flink-cep:run
```

## Project Conventions

### Code Style
- Follow Google Java Style Guide
- Use 4-space indentation
- Maximum line length: 120 characters
- Use meaningful variable and method names

### Documentation
- All architectural decisions documented in arc42 Chapter 9
- All runtime scenarios documented in arc42 Chapter 6
- Use PlantUML for diagrams
- Use AsciiDoc for documentation
- Follow docs-as-code-toolkit/architecture-knowledge-toolkit structure

### Testing
- Unit tests for business logic
- Integration tests for component interactions
- Each test references its use case ID for traceability

## Comparison Focus Areas

### 1. Scalability [semantic-anchor: quality.scalability]
- Horizontal scaling capabilities
- Partitioning strategies
- Consumer group behavior
- Resource utilization under load

### 2. Operational Complexity
- Deployment requirements
- Monitoring and management
- Failure recovery
- Configuration complexity

### 3. State Handling
- State management approaches
- Checkpointing and persistence
- State size and memory usage
- State recovery after failures

### 4. Rule Complexity
- Rule definition syntax
- Rule maintainability
- Rule testing
- Rule performance impact

### 5. Architectural Trade-offs
- Technology maturity
- Community and ecosystem
- Vendor lock-in potential
- Integration complexity

## Risks and Mitigations

| Risk | Probability | Impact | Priority | Mitigation |
|------|-------------|--------|----------|------------|
| State explosion in CEP | Medium | High | High | Limit window sizes, use efficient patterns [semantic-anchor: risk.state-explosion] |
| Technology compatibility issues | Low | Medium | Medium | Use stable versions, test integration early [semantic-anchor: risk.technology-compatibility] |
| Operational complexity too high | Medium | Medium | Medium | Keep implementations minimal, document thoroughly [semantic-anchor: risk.operational-complexity] |

## Architecture Documentation

For detailed architecture documentation, see link:src/docs/doc-001-arc42.adoc[arc42 Architecture Documentation].

The architecture documentation follows the arc42 template with architecture-knowledge-toolkit extensions and includes:
- link:src/docs/doc-002-vision-mission.adoc[Vision and Mission]
- link:src/docs/doc-004-roadmap.adoc[Project Roadmap]
- link:src/docs/doc-005-questions-and-answers.adoc[Questions and Answers]
- The arc42 chapters under link:src/docs/arc42/[src/docs/arc42/], including 15 quality scenarios, 6 risks, and 6 technical-debt items

## Semantic Anchors

This project uses semantic anchors for cross-referencing across documentation and code:

- `[semantic-anchor: event-processing-lab]` - Project root
- `[semantic-anchor: component.*]` - Component identifiers
- `[semantic-anchor: technology.*]` - Technology identifiers
- `[semantic-anchor: decision.*]` - Architecture decisions
- `[semantic-anchor: quality.*]` - Quality attributes
- `[semantic-anchor: risk.*]` - Risks
- `[semantic-anchor: scenario.*]` - Runtime scenarios

## Related Resources

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Esper Documentation](https://www.espertech.com/esper/documentation.html)
- [Apache Flink Documentation](https://nightlies.apache.org/flink/flink-docs-stable/)
- [Flink CEP Documentation](https://nightlies.apache.org/flink/flink-docs-stable/docs/libs/cep/)
- [arc42 Architecture Template](https://arc42.org/)
- [docs-as-code-toolkit/architecture-knowledge-toolkit](https://github.com/arc42/docs-as-code-toolkit)

---

*Project Status: Active Development*  
*Last Updated: 2026-07-02*  
*Maintainer: Integration Architecture Team*  
*Documentation: Following docs-as-code-toolkit/architecture-knowledge-toolkit structure*
