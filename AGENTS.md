# Event Processing Lab - Agent Instructions

[semantic-anchor: event-processing-lab]

This document provides instructions for AI agents working on the Event Processing Lab project.

## Project Context

The Event Processing Lab is a **learning project** designed to compare Kafka, Esper, Apache Flink, and Flink CEP from the perspective of an integration architect. It focuses on understanding the architectural trade-offs, scalability characteristics, operational complexity, state handling, and rule complexity of each technology.

[semantic-anchor: quality.learnability]
[semantic-anchor: quality.understandability]

## Primary Goals

1. **Learning Value**: Enable hands-on comparison of event processing technologies
2. **Understandability**: Keep code and architecture simple and well-documented
3. **Modifiability**: Allow easy experimentation with different configurations
4. **Observability**: Include logging and metrics for understanding behavior

## Architecture Documentation Standards

This project follows the **arc42** architecture documentation template with extensions from the **docs-as-code-toolkit/architecture-knowledge-toolkit**.

The documentation structure follows the architecture-knowledge-toolkit pattern with:
- YAML front matter for metadata and traceability relations
- AsciiDoc format with explicit cross-references
- Draft/proposed status with reviewed:false for AI-assisted content
- Explicit traceability between documentation elements
- Semantic anchors for cross-referencing

### Required Documentation Structure

```
src/docs/
├── arc42/                          # arc42 architecture documentation
│   ├── 01-introduction-and-goals.adoc
│   ├── 02-architecture-constraints.adoc
│   ├── 03-system-scope-and-context.adoc
│   ├── 04-solution-strategy.adoc
│   ├── 05-building-block-view.adoc
│   ├── 06-runtime-view.adoc
│   ├── 07-deployment-view.adoc
│   ├── 08-crosscutting-concepts.adoc
│   ├── 09-architecture-decisions/   # ADRs (Architecture Decision Records)
│   │   ├── ADR-001.adoc            # Use Kafka as Event Backbone
│   │   ├── ADR-002.adoc            # Use JSON Serialization
│   │   ├── ADR-003.adoc            # Plain Java over Spring Boot
│   │   └── ADR-004.adoc            # Separate Implementations
│   ├── 10-quality-requirements/     # Quality scenarios
│   │   ├── QS-001.adoc through QS-015.adoc
│   ├── 11-risks-and-technical-debt/
│   │   ├── RISK-001.adoc through RISK-006.adoc
│   │   └── TD-001.adoc through TD-010.adoc
│   └── 12-glossary.adoc
├── arc42.adoc                      # Entry point for assembled documentation
├── canvas/                         # Business and architecture canvases
│   ├── architecture-inception-canvas.adoc
│   ├── architecture-communication-canvas.adoc
│   ├── business-model-canvas.adoc
│   ├── techstack-canvas.adoc
│   └── value-proposition-canvas.adoc
├── fragments/                      # Reusable documentation fragments
│   ├── architecture/
│   │   ├── adr-template.adoc
│   │   ├── quality-attributes.adoc
│   │   ├── quality-scenario-template.adoc
│   │   └── risk-assessment-template.adoc
│   └── product/
│       ├── event-types.adoc
│       └── technology-comparison.adoc
├── questions-and-answers.adoc
├── roadmap.adoc
└── vision-mission.adoc
```

### ADR Format (Nygard with Pugh Matrix)

Each ADR follows the Nygard format with Pugh Matrix analysis:
- YAML front matter with metadata (id, type, title, status, owner, created, reviewed, summary, tags, relations)
- Context section describing the problem
- Decision section with chosen solution
- Alternatives considered with pros/cons
- Consequences section with positive and negative outcomes
- Pugh Matrix for decision analysis
- Explicit traceability relations to other documentation elements
- Semantic anchors for cross-referencing

### Quality Scenarios (Six-Part Form)

Each quality scenario follows the six-part quality attribute scenario form:
1. **Source**: The stakeholder or system element that originates the stimulus
2. **Stimulus**: The event or condition that triggers the scenario
3. **Artifact**: The system element that is affected
4. **Environment**: The conditions under which the scenario occurs
5. **Response**: The expected behavior of the system
6. **Response Measure**: The measurable criterion with literal figures

### Risk Documentation

Each risk includes:
- YAML front matter with metadata
- Risk statement in the form: "Because <cause>, <event> may occur, leading to <impact>"
- Assessment table with likelihood, impact, priority, timeframe, confidence
- Mitigation options
- Traceability relations to other documentation elements
- Semantic anchors

### Technical Debt Documentation

Each technical debt item includes:
- YAML front matter with metadata
- Statement describing the debt
- Details (module, impact, priority, type)
- Resolution description
- Acceptance criteria
- Traceability relations
- Semantic anchors

## Semantic Anchors

Use semantic anchors extensively for cross-referencing across documentation and code:

```
[semantic-anchor: event-processing-lab]
[semantic-anchor: component.event-model]
[semantic-anchor: component.event-producer]
[semantic-anchor: component.kafka-consumer]
[semantic-anchor: component.esper-cep]
[semantic-anchor: component.flink-basic]
[semantic-anchor: component.flink-cep]

[semantic-anchor: technology.kafka]
[semantic-anchor: technology.esper]
[semantic-anchor: technology.flink]
[semantic-anchor: technology.flink-cep]

[semantic-anchor: decision.kafka-as-event-backbone]
[semantic-anchor: decision.json-serialization]
[semantic-anchor: decision.plain-java-over-spring]
[semantic-anchor: decision.separate-implementations]

[semantic-anchor: quality.scalability]
[semantic-anchor: quality.understandability]
[semantic-anchor: quality.learnability]
[semantic-anchor: quality.modifiability]
[semantic-anchor: quality.observability]

[semantic-anchor: scenario.order-creation]
[semantic-anchor: scenario.payment-processing]
[semantic-anchor: scenario.payment-timeout]
[semantic-anchor: scenario.order-cancellation]
[semantic-anchor: scenario.shipment-start]

[semantic-anchor: risk.state-explosion]
[semantic-anchor: risk.operational-complexity]
[semantic-anchor: risk.technology-compatibility]
[semantic-anchor: risk.event-loss]
[semantic-anchor: risk.performance-bottleneck]
[semantic-anchor: risk.learning-curve-too-steep]
```

## Cross-Cutting Concepts (arc42 Chapter 8)

### 8.1 Threat Model
- STRIDE methodology
- Each threat gets a unique ID (T-001, T-002, ...)
- Threat IDs are referenced by mitigations

### 8.2 Security
- Every mitigation references T-IDs it closes

### 8.3 Test
- Testing pyramid approach
- Tests trace to Use Cases and Business Rules

### 8.4 Observability
- Logging, metrics, traces, audit trails
- Cross-reference with quality scenarios

### 8.5 Error Handling
- Retry, circuit breaker, fallback, recovery strategies

## Quality Goals (arc42 Chapter 1.2)

List only top 3-5 quality goals that drive architecture decisions:

1. **Learnability** [semantic-anchor: quality.learnability]
   - Enable quick understanding of each technology's capabilities
   - Provide clear examples and comparisons

2. **Understandability** [semantic-anchor: quality.understandability]
   - Simple, well-documented code
   - Clear architecture diagrams

3. **Scalability** [semantic-anchor: quality.scalability]
   - Demonstrate how each technology scales
   - Show throughput and latency characteristics

4. **Modifiability** [semantic-anchor: quality.modifiability]
   - Easy to experiment with different configurations
   - Flexible architecture for comparisons

5. **Observability** [semantic-anchor: quality.observability]
   - Logging for understanding behavior
   - Metrics for performance measurement

Chapter 10 may elaborate on additional quality characteristics, each marked as either concretising a Chapter 1.2 goal or as derived.

## Traceability Contract

- Every Chapter 1.2 quality goal maps to a named approach in Chapter 4
- External systems in Chapter 3 (context) match Chapter 5 Level-1 building blocks
- Every Chapter 5 building block appears in at least one Chapter 6 runtime scenario
- Chapter 6 includes at least one error/recovery scenario
- Chapter 9 carries an in-document ADR index
- Each Chapter 5 building block states responsibility, interface, and source location
- Every Chapter 1.2 quality goal maps to quality scenarios in Chapter 10
- Each Chapter 10 scenario cross-links to Chapter 1.2 goal it concretises

## Documentation Workflow

### Phase 1: Analysis
1. Analyze the use case
2. Identify quality attributes
3. Identify architectural risks
4. Identify ADR candidates
5. Create initial architecture documentation

### Phase 2: Implementation
1. Create code based on documented architecture
2. Ensure tests trace to use cases
3. Maintain documentation as code evolves

### Phase 3: Validation
1. Verify all traceability requirements are met
2. Ensure all building blocks appear in runtime scenarios
3. Validate ADR decisions are implemented correctly

## Coding Standards

### Technology Stack
- Java 21
- Gradle (multi-module)
- Kafka as event backbone [semantic-anchor: technology.kafka]
- JSON serialization (first iteration) [semantic-anchor: decision.json-serialization]
- Plain Java preferred over Spring Boot [semantic-anchor: decision.plain-java-over-spring]

### Module Structure
```
event-processing-lab/
├── event-model/           [semantic-anchor: component.event-model]
├── event-producer/        [semantic-anchor: component.event-producer]
├── kafka-consumer/        [semantic-anchor: component.kafka-consumer]
├── esper-cep/             [semantic-anchor: component.esper-cep]
├── flink-basic/           [semantic-anchor: component.flink-basic]
└── flink-cep/             [semantic-anchor: component.flink-cep]
```

### Code Quality
- Follow SOLID principles
- DRY, KISS
- Ubiquitous Language from Domain-Driven Design
- Use semantic anchors in code comments where appropriate

## Use Case: Order Lifecycle

[semantic-anchor: scenario.order-creation]
[semantic-anchor: scenario.payment-processing]
[semantic-anchor: scenario.payment-timeout]
[semantic-anchor: scenario.order-cancellation]
[semantic-anchor: scenario.shipment-start]

### Event Types
- `OrderCreated`
- `PaymentReceived`
- `PaymentFailed`
- `OrderCancelled`
- `ShipmentStarted`

### Learning Goals by Technology

#### Kafka [semantic-anchor: technology.kafka]
- Event transport and event backbone capabilities
- Partitioning and ordering guarantees
- Consumer group behavior
- Scalability characteristics

#### Esper [semantic-anchor: technology.esper]
- Lightweight CEP engine capabilities
- Rule definition and complexity
- State handling
- Performance characteristics

#### Flink [semantic-anchor: technology.flink]
- Distributed stream processing
- State management
- Checkpointing
- Scalability

#### Flink CEP [semantic-anchor: technology.flink-cep]
- Pattern detection on top of Flink
- Comparison with Esper
- State handling differences
- Complexity trade-offs

## Comparison Dimensions

1. **Scalability**: How each technology scales with increasing event volume
2. **Operational Complexity**: Deployment, monitoring, maintenance requirements
3. **State Handling**: How state is managed and persisted
4. **Rule Complexity**: Complexity of defining and maintaining rules/patterns
5. **Architectural Trade-offs**: Pros and cons of each approach

## Required ADRs

1. **ADR-001: Use Kafka as Event Backbone** [semantic-anchor: decision.kafka-as-event-backbone]
   - Status: Accepted
   - Decision: Use Apache Kafka as the central event transport mechanism
   - Consequences: Provides high throughput, fault tolerance, and scalability

2. **ADR-002: Use JSON Serialization for First Iteration** [semantic-anchor: decision.json-serialization]
   - Status: Accepted
   - Decision: Use JSON for event serialization in the initial implementation
   - Consequences: Human-readable, easy to debug, but less efficient than binary formats

3. **ADR-003: Keep Spring Boot Optional, Use Plain Java** [semantic-anchor: decision.plain-java-over-spring]
   - Status: Accepted
   - Decision: Prefer plain Java implementations where simpler
   - Consequences: Reduces dependencies, keeps examples focused on core concepts

4. **ADR-004: Separate Esper and Flink Implementations** [semantic-anchor: decision.separate-implementations]
   - Status: Accepted
   - Decision: Maintain separate implementations for comparison
   - Consequences: Allows direct comparison, but may lead to some code duplication

## Agent Behavior

When working on this project:

1. **Always** follow the analysis-first approach:
   - Analyze use case before coding
   - Identify quality attributes
   - Identify risks
   - Identify ADR candidates
   - Create/update documentation
   - Then implement code

2. **Always** use semantic anchors for cross-referencing

3. **Always** maintain traceability between documentation elements

4. **Always** keep code minimal and focused on learning value

5. **Prefer** learning value over production completeness

6. **Ensure** all runtime scenarios are implemented across technologies

7. **Document** architectural decisions and their consequences
8. **Follow** the architecture-knowledge-toolkit structure and conventions
9. **Use** YAML front matter in all documentation files
10. **Include** explicit traceability relations in all documentation
11. **Mark** AI-assisted content with reviewed:false and appropriate status

## Documentation References

- [arc42 Template](https://arc42.org/)
- [docs-as-code-toolkit](https://github.com/arc42/docs-as-code-toolkit)
- [architecture-knowledge-toolkit](https://github.com/arc42/docs-as-code-toolkit/tree/main/architecture-knowledge-toolkit)

### Project Architecture Documentation

The complete architecture documentation for this project is available in `src/docs/arc42.adoc`, which serves as the entry point for the assembled architecture documentation following the arc42 template and architecture-knowledge-toolkit conventions.
