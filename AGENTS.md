# PROJECT KNOWLEDGE BASE

Generated: 2026-01-20 14:14:53Z
Commit: 456c1873
Branch: update-to-micronaut-5

## OVERVIEW
Micronaut + Kafka integrations and utilities. Multi-project Gradle build (Java/Groovy/Kotlin). Modules: core Kafka client support, Kafka Streams, BOM, build logic, multi-language test suites.

## STRUCTURE
```
micronaut-kafka/
├── kafka/               # Core Micronaut Kafka (clients, config, binders, metrics)
├── kafka-streams/       # Micronaut Kafka Streams integration
├── kafka-bom/           # Dependency management (Bill of Materials)
├── buildSrc/            # Internal Gradle build logic/plugins for this repo
├── test-suite/          # Java-based tests and docs snippets
├── test-suite-groovy/   # Groovy-based tests and docs snippets (Spock/JUnit)
├── test-suite-kotlin/   # Kotlin-based tests and docs snippets
├── tests/               # Standalone sample app/tests (e.g., tasks-sasl-plaintext)
└── src/main/docs/       # Project documentation sources
```

## WHERE TO LOOK
| Task | Location | Notes |
|------|----------|-------|
| Kafka client annotations, config, metrics | kafka/ | Core features used by apps; majority of source here |
| Kafka Streams integration | kafka-streams/ | Streams-specific factories, events, config |
| Version alignment | kafka-bom/ | Declares managed dependency versions |
| Build conventions, test wiring | buildSrc/ | Internal Gradle logic shared across modules |
| Example tests (Java) | test-suite/ | Doc-driven tests, snippets |
| Example tests (Groovy) | test-suite-groovy/ | Spock/JUnit examples |
| Example tests (Kotlin) | test-suite-kotlin/ | Kotlin examples |
| Standalone sample with Testcontainers Kafka | tests/ | tasks-sasl-plaintext module |
| Documentation content | src/main/docs/ | Guide and snippets source |

## CODE MAP
LSP workspace symbols unavailable in this environment. Use IDE to surface classes under io.micronaut.configuration.kafka* and io.micronaut.configuration.kafka.streams*.

## CONVENTIONS
- Gradle multi-project with wrapper (./gradlew). Modules have individual build.gradle files.
- Tests rely on Testcontainers for Kafka in several modules; MicronautTest is the primary test annotation.
- Java primary in modules; Groovy tests common; Kotlin tests present in dedicated module.
- BOM module centralizes dependency versions; library modules depend on it.
- Package roots:
  - Core: io.micronaut.configuration.kafka
  - Streams: io.micronaut.configuration.kafka.streams

## ANTI-PATTERNS (THIS PROJECT)
- Do not use automated attempt_completion without running tests (see .clinerules and .github instructions).
- Avoid filing Q&A as issues; use StackOverflow/Gitter (repo policy).

## UNIQUE STYLES
- Build customization via buildSrc (shared Gradle logic for tests/modules).
- Separate test suites by language for documentation coverage and examples.
- Events pattern for consumer/streams lifecycle (AbstractKafkaApplicationEvent, etc.).

## COMMANDS
```bash
# Build all
./gradlew build

# Run tests (all / per-module)
./gradlew test
./gradlew :kafka:test
./gradlew :kafka-streams:test

# Publish to local Maven (for local consumers)
./gradlew publishToMavenLocal

# Check (lint/tests where configured)
./gradlew check
```

## NOTES
- Tests using Testcontainers need Docker available.
- When changing build logic, verify buildSrc first; many modules import shared conventions.
- For dependency changes, update kafka-bom and align module constraints accordingly.
