# Architecture Decision Records (ADRs)

[semantic-anchor: event-processing-lab.adr]

This directory contains the Architecture Decision Records (ADRs) for the Event Processing Lab project. ADRs document the architectural decisions made during the project's development, including the context, alternatives considered, decision rationale, and consequences.

## ADR Index

| ADR ID | Title | Status | Semantic Anchor | Tags |
|--------|-------|--------|-----------------|------|
| [ADR-001](ADR-001.md) | Use Kafka as Event Backbone | ✅ Accepted | [semantic-anchor: decision.kafka-as-event-backbone] | kafka, event-backbone, infrastructure, core |
| [ADR-002](ADR-002.md) | Use JSON Serialization for First Iteration | ✅ Accepted | [semantic-anchor: decision.json-serialization] | serialization, json, event-format, data |
| [ADR-003](ADR-003.md) | Keep Spring Boot Optional, Use Plain Java | ✅ Accepted | [semantic-anchor: decision.plain-java-over-spring] | spring-boot, plain-java, framework, simplicity |
| [ADR-004](ADR-004.md) | Separate Esper and Flink Implementations | ✅ Accepted | [semantic-anchor: decision.separate-implementations] | comparison, esper, flink, architecture, separation |

## ADR Template

All ADRs in this project follow an extended version of the [Nygard ADR template](https://adr.github.io/madr/) with the following sections:

1. **Status**: Current status (Proposed, Accepted, Deprecated, Superseded)
2. **Context**: The problem being addressed
3. **Decision**: The chosen solution
4. **Decision Drivers**: Factors influencing the decision
5. **Alternatives Considered**: Other options evaluated
6. **Pugh Matrix Analysis**: Quantitative comparison of alternatives
7. **Consequences**: Positive and negative outcomes of the decision
8. **Risk Mitigations**: Strategies to address identified risks
9. **Related Decisions**: Connections to other ADRs
10. **Related Components**: Project components affected by this decision
11. **Related Technologies**: Technologies involved or considered
12. **Related Quality Goals**: Quality attributes this decision supports
13. **Related Scenarios**: Runtime scenarios affected or enabled
14. **Metadata**: Administrative information (ID, creation date, author, etc.)

## ADR Workflow

### 1. Proposing a New ADR

1. Create a new file `ADR-XXX.md` where XXX is the next available number
2. Use the ADR template (see below)
3. Fill in the context, alternatives, and analysis
4. Set status to "Proposed"
5. Submit for review via pull request

### 2. Review Process

1. Team members review the ADR for:
   - Completeness of analysis
   - Accuracy of information
   - Appropriateness of the decision
   - Alignment with project goals
2. Discuss in team meeting or async review
3. Address feedback and update ADR as needed

### 3. Acceptance

1. Once consensus is reached, update status to "Accepted"
2. Update any related documentation
3. Merge the ADR to the main branch

### 4. Implementation

1. Implement the decision as described in the ADR
2. Update the ADR with any implementation learnings
3. Ensure all related components are updated

### 5. Deprecation/Supersession

1. If a decision needs to be changed, create a new ADR that supersedes the old one
2. Update the old ADR status to "Deprecated" or "Superseded"
3. Reference the new ADR in the old one

## ADR Template

```markdown
# ADR-XXX: Decision Title

[semantic-anchor: decision.decision-name]

## Status
[✅ Accepted / 🔄 Proposed / ❌ Deprecated / 🔄 Superseded]

## Context

[Describe the problem being addressed and why it needs a decision]

## Decision

[State the decision clearly]

## Decision Drivers

| Driver | Importance | Notes |
|--------|------------|-------|
| [Driver 1] | [High/Medium/Low] | [Description] |

## Alternatives Considered

### 1. [Alternative 1] [✅ Selected / ❌ Rejected]
- **Pros**: [List advantages]
- **Cons**: [List disadvantages]
- **Integration**: [Integration considerations]

### 2. [Alternative 2] [❌ Rejected]
- **Pros**: [List advantages]
- **Cons**: [List disadvantages]

## Pugh Matrix Analysis

| Criteria | Weight | Alt 1 | Alt 2 | Alt 3 |
|----------|--------|-------|-------|-------|
| [Criterion] | [Weight] | [Score] | [Score] | [Score] |
| **Total** | | [Total] | [Total] | [Total] |

## Consequences

### Positive Consequences
1. [Benefit 1]
2. [Benefit 2]

### Negative Consequences
1. [Drawback 1]
2. [Drawback 2]

## Risk Mitigations

| Risk | Mitigation | Status |
|------|------------|--------|
| [Risk 1] | [Mitigation strategy] | [✅ / ⚠️ / ❌] |

## Related Decisions

- [ADR-XXX: Related Decision](ADR-XXX.md) [semantic-anchor: decision.related]

## Related Components

- [component.name](../architecture.adoc#building-blocks) [semantic-anchor: component.name]

## Related Technologies

- [Technology](url) [semantic-anchor: technology.name]

## Related Quality Goals

- [Quality Goal](../../architecture.adoc#quality-goals) [semantic-anchor: quality.name]

## Related Scenarios

- [Scenario Name](../../architecture.adoc#runtime-scenarios) [semantic-anchor: scenario.name]

## Metadata

- **ADR ID**: ADR-XXX
- **Created**: YYYY-MM-DD
- **Status**: [Status]
- **Author**: [Author]
- **Supersedes**: [ADR-XXX if applicable]
- **Superseded By**: [ADR-XXX if applicable]
- **Tags**: [comma-separated, tags]

---

*This ADR follows the extended Nygard template for the Event Processing Lab project.*
```

## ADR Status Definitions

| Status | Icon | Description |
|--------|------|-------------|
| **Proposed** | 🔄 | Decision is being discussed, not yet finalized |
| **Accepted** | ✅ | Decision has been agreed upon and is being implemented |
| **Deprecated** | ❌ | Decision has been reversed, no longer valid |
| **Superseded** | 🔄 | Decision has been replaced by a newer ADR |

## Semantic Anchors in ADRs

All ADRs in this project use semantic anchors for cross-referencing:

- **Decision Anchors**: `[semantic-anchor: decision.*]` - Reference specific architectural decisions
- **Component Anchors**: `[semantic-anchor: component.*]` - Reference project components
- **Technology Anchors**: `[semantic-anchor: technology.*]` - Reference technologies used
- **Quality Anchors**: `[semantic-anchor: quality.*]` - Reference quality attributes
- **Scenario Anchors**: `[semantic-anchor: scenario.*]` - Reference runtime scenarios
- **Risk Anchors**: `[semantic-anchor: risk.*]` - Reference identified risks

## ADR Statistics

- **Total ADRs**: 4
- **Accepted**: 4
- **Proposed**: 0
- **Deprecated/Superseded**: 0
- **Last Updated**: 2026-07-02

## Cross-Reference Guide

### By Technology

- **[Kafka](ADR-001.md)**: ADR-001
- **[JSON](ADR-002.md)**: ADR-002
- **[Plain Java](ADR-003.md)**: ADR-003
- **[Esper/Flink](ADR-004.md)**: ADR-004

### By Quality Goal

- **[Learnability](ADR-001.md)**: ADR-001, ADR-002, ADR-003, ADR-004
- **[Understandability](ADR-001.md)**: ADR-001, ADR-002, ADR-003, ADR-004
- **[Scalability](ADR-001.md)**: ADR-001
- **[Modifiability](ADR-003.md)**: ADR-003, ADR-004

### By Component

- **[event-model](ADR-002.md)**: ADR-002
- **[event-producer](ADR-001.md)**: ADR-001
- **[kafka-consumer](ADR-001.md)**: ADR-001
- **[esper-cep](ADR-004.md)**: ADR-004
- **[flink-basic/flink-cep](ADR-004.md)**: ADR-004

---

*For more information about ADRs, see [adr.github.io](https://adr.github.io/)*
