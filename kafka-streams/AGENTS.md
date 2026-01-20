# MODULE KNOWLEDGE BASE — kafka-streams

Generated: 2026-01-20 14:14:53Z

## OVERVIEW
Micronaut Kafka Streams integration: factory/configuration, events, health, metrics, interactive query.

## STRUCTURE
```
kafka-streams/
└── src/
    ├── main/java/io/micronaut/configuration/kafka/streams/
    │   ├── KafkaStreamsFactory.java, KafkaStreamsConfiguration.java, DefaultKafkaStreamsConfiguration.java
    │   ├── ConfiguredStreamBuilder.java, InteractiveQueryService.java
    │   ├── event/     # Streams lifecycle events
    │   ├── health/    # Health indicators
    │   └── metrics/   # Micrometer metrics for Streams
    └── test/groovy/io/micronaut/configuration/kafka/streams/
        ├── health/, metrics/, listeners/, uncaught/
        └── samples: wordcount/, startkafkastreams/, optimization/
```

## WHERE TO LOOK
| Task | Location | Notes |
|------|----------|-------|
| Create Streams app | KafkaStreamsFactory, ConfiguredStreamBuilder | Bean wiring, builder injection |
| Configure Streams | *KafkaStreamsConfiguration classes* | Micronaut→Kafka Streams config mapping |
| Interactive queries | InteractiveQueryService | Access local state stores |
| Health/Metrics | health/, metrics/ | Micrometer + indicators |
| Events | event/ | AbstractKafkaStreamsEvent hierarchy |
| Examples/tests | src/test/groovy/... | WordCount, uncaught exception handling |

## CONVENTIONS (module-specific)
- Factory-based Streams bootstrapping; configuration beans map Kafka Streams props.
- Tests: Groovy @MicronautTest + Testcontainers Kafka.

## ANTI-PATTERNS (module-specific)
- Don’t start Streams manually outside the factory; rely on managed lifecycle.
- Don’t hardcode Streams config; use configuration beans.
