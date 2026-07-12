# Event Processing Lab

*A hands-on learning project comparing Kafka, Esper, Apache Flink, and Flink CEP for event processing.*

## Overview

The Event Processing Lab lets integration architects gain practical experience
with different event processing technologies through one common use case: the
**order lifecycle** (`OrderCreated` → `PaymentReceived` / `PaymentFailed` →
`ShipmentStarted` / `OrderCancelled`). By implementing the same scenarios across
several technologies, it enables direct comparison of scalability, operational
complexity, state handling, and rule complexity.

The architecture, goals, decisions, quality scenarios, risks, and the
**Comparison Matrix** live in the arc42 documentation under
[`src/docs/`](src/docs/) — see [Architecture documentation](#architecture-documentation).
This README covers only how to set up, build, and run the lab.

## Prerequisites

- Java 21 or later
- Gradle 8.5 or later (or use a locally installed Gradle; the wrapper jar is not committed)
- Apache Kafka (a local broker via the provided Docker Compose file is easiest)
- A container engine (Docker or Podman) if you want to build the docs with `./build.sh`

## Setup

### 1. Start Kafka

```bash
docker-compose -f docker-compose.kafka.yml up -d
```

Kafka defaults to `localhost:9092`; override via `gradle.properties`
(`kafka.bootstrap.servers`).

### 2. Build

```bash
gradle build            # build and test all modules
gradle :event-model:build
```

## Running the components

Each processing technology is a runnable module:

```bash
gradle :event-producer:run     # produce order-lifecycle events
gradle :kafka-consumer:run     # plain Kafka consumer baseline
gradle :esper-cep:run          # Esper CEP engine
gradle :flink-basic:run        # Flink DataStream processing
gradle :flink-cep:run          # Flink CEP pattern detection
```

## Running a use case to evaluate the Comparison Matrix

The [Comparison Matrix](src/docs/arc42/doc-04000-solution-strategy.adoc) rates
each technology along the comparison dimensions. Its values are a reviewable
assessment — reproduce and evaluate them by running the shared use case against
each processor:

1. **Start Kafka** (see Setup) so the `order-events` topic is available.
2. **Start one processor** to evaluate, e.g. the Esper CEP engine:
   ```bash
   gradle :esper-cep:run
   ```
3. **Emit an order-lifecycle scenario** from a second terminal:
   ```bash
   gradle :event-producer:run
   ```
   The producer publishes the order lifecycle events (`OrderCreated`,
   `PaymentReceived`/`PaymentFailed`, `ShipmentStarted`, `OrderCancelled`) that
   drive the runtime scenarios in
   [Chapter 6 — Runtime View](src/docs/arc42/doc-06000-runtime-view.adoc).
4. **Observe the processor output** (console/logs): which patterns it detects,
   how it keeps state, and the latency/throughput it reports. This is the
   evidence for the comparison dimensions.
5. **Repeat for each processor** (`kafka-consumer`, `esper-cep`, `flink-basic`,
   `flink-cep`) and compare the observations against the Comparison Matrix and
   the measurable quality scenarios in
   [Chapter 10 — Quality Requirements](src/docs/arc42/doc-10000-quality-requirements.adoc).

> Metrics are currently read from logs. An automated dashboard that surfaces the
> comparison metrics is on the [roadmap](src/docs/doc-004-roadmap.adoc)
> (Phase 5), and unit/behaviour tests for the implementations are planned in
> Phase 4.

## Project structure

```
event-processing-lab/
├── AGENTS.md                      # Thin agent contract routing to the toolkit
├── README.md                      # This file
├── build.sh                       # docs-toolbox task runner (validate/generate/build)
├── build.gradle / settings.gradle # Gradle multi-module build
├── metamodel/                     # Vendored artifact/relation schemas (contracts)
├── templates/                     # Vendored ADR/quality-scenario/risk templates
├── scripts/                       # Vendored validator + agent-adapter generators
├── adapters/                      # Generated agent adapters (codex, vibe, copilot, cursor)
├── features/                      # Gherkin behaviour specs (living documentation)
├── .github/workflows/             # CI (validate + tests) and GitHub Pages publish
├── src/docs/                      # arc42 architecture documentation (see below)
├── event-model/                   # Shared event model and serialization
├── event-producer/                # Kafka producer of order-lifecycle events
├── kafka-consumer/                # Plain Kafka consumer baseline
├── esper-cep/                     # Esper CEP engine
├── flink-basic/                   # Flink DataStream processing
└── flink-cep/                     # Flink CEP pattern detection
```

Each module holds its Java sources under `src/main/java/com/example/<module>/`.
Derived documentation output under `src/docs/**/generated/` and `build/` is not
committed; regenerate it with `./build.sh`.

## Technologies

| Technology | Purpose | Module |
|------------|---------|--------|
| [Kafka](https://kafka.apache.org/) | Event backbone & transport | event-producer, kafka-consumer |
| [Esper](https://www.espertech.com/esper/) | Lightweight CEP engine | esper-cep |
| [Apache Flink](https://flink.apache.org/) | Distributed stream processing | flink-basic, flink-cep |
| [Flink CEP](https://nightlies.apache.org/flink/flink-docs-stable/docs/libs/cep/) | Pattern detection library | flink-cep |

## Architecture documentation

The complete architecture documentation follows the arc42 template and the
`docs-as-code-toolkit/architecture-knowledge-toolkit` conventions:

- [arc42 Architecture Documentation](src/docs/doc-001-arc42.adoc) — assembled entry point
- [Solution Strategy & Comparison Matrix](src/docs/arc42/doc-04000-solution-strategy.adoc)
- [Runtime View](src/docs/arc42/doc-06000-runtime-view.adoc) — the runtime scenarios
- [Quality Requirements](src/docs/arc42/doc-10000-quality-requirements.adoc) — quality goals and 15 measurable scenarios
- [Architecture Decisions](src/docs/arc42/doc-09000-architecture-decisions.adoc) — ADRs
- [Risks and Technical Debt](src/docs/arc42/doc-11000-risks-and-technical-debt.adoc)
- [Vision & Mission](src/docs/doc-002-vision-mission.adoc), [Roadmap](src/docs/doc-004-roadmap.adoc), [Q&A](src/docs/doc-005-questions-and-answers.adoc)

### Validating and building the documentation

The docs are validated, generated, and rendered through the pinned
`docs-toolbox` container image via `./build.sh`:

```bash
./build.sh validate    # validate artifact metadata and relations
./build.sh generate    # regenerate derived fragments and indexes
./build.sh build       # render build/architecture/index.html
```

Set `DOCS_TOOLBOX_LOCAL=1` to run against the host toolchain instead. CI
(`.github/workflows/ci.yml`) validates every pull request and builds the Java
modules; on `main`, the documentation is published to GitHub Pages
(`.github/workflows/pages.yml`).

## Related resources

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Esper Documentation](https://www.espertech.com/esper/documentation.html)
- [Apache Flink Documentation](https://nightlies.apache.org/flink/flink-docs-stable/)
- [Flink CEP Documentation](https://nightlies.apache.org/flink/flink-docs-stable/docs/libs/cep/)
- [arc42 Architecture Template](https://arc42.org/)
- [docs-as-code-toolkit / architecture-knowledge-toolkit](https://github.com/docs-as-code-toolkit/architecture-knowledge-toolkit)
