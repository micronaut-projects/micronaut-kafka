# MODULE KNOWLEDGE BASE — buildSrc

Generated: 2026-01-20 14:14:53Z

## OVERVIEW
Internal Gradle build logic: shared module conventions and test wiring for Micronaut Kafka projects.

## STRUCTURE
```
buildSrc/
├── build.gradle, settings.gradle
└── src/main/groovy/
    ├── io.micronaut.build.internal.kafka-base.gradle
    ├── io.micronaut.build.internal.kafka-module.gradle
    ├── io.micronaut.build.internal.kafka-tests.gradle
    └── io.micronaut.internal.build.kafka-testsuite.gradle
```

## WHERE TO LOOK
| Task | Location | Notes |
|------|----------|-------|
| Base module conventions | kafka-base.gradle | Common Gradle setup shared across modules |
| Module-specific wiring | kafka-module.gradle | Adds Micronaut modules, test runtime deps |
| Tests (module) | kafka-tests.gradle | Awaitility, Micronaut HTTP client, etc. |
| Tests (test-suite) | kafka-testsuite.gradle | Wires streams, serde, reactor, logging runtime |

## CONVENTIONS (module-specific)
- Centralizes Micronaut-related dependencies via version catalogs and BOMs.
- Encodes standard test/runtime deps for modules and test-suites (MicronautTest, Testcontainers Kafka, Logback, JUnit Platform).
- Keep module build.gradle lean—import internal scripts instead of duplicating logic.

## ANTI-PATTERNS (module-specific)
- Don’t re-declare test deps in individual modules; use shared scripts.
- Don’t alter plugin versions per-module; keep in buildSrc for consistency.
