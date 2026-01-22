# MODULE KNOWLEDGE BASE — kafka

Generated: 2026-01-20 14:14:53Z

## OVERVIEW
Micronaut Kafka core: clients, annotations, config, binders, metrics, events, factories.

## STRUCTURE
```
kafka/
└── src/
    ├── main/java/io/micronaut/configuration/kafka/
    │   ├── annotation/   # @KafkaListener, @KafkaClient, @Topic, etc.
    │   ├── bind/         # Argument/Body binders for Kafka messaging
    │   ├── config/       # Client/consumer/producer properties mapping
    │   ├── processor/    # Annotation processors / factories
    │   ├── metrics/      # Micrometer metrics integration
    │   ├── event/        # Lifecycle events (consumer start/poll/subscribed)
    │   ├── seek/         # Seek operations and helpers
    │   ├── serde/        # Serde abstractions and registry hooks
    │   ├── intercept/, retry/, tracing/, reactor/, health/, exceptions/
    │   └── ...
    └── test/groovy/io/micronaut/configuration/kafka/
        ├── serde/serderegistry/
        ├── offsets/
        ├── errors/
        ├── metrics/
        ├── event/
        └── seek/
```

## WHERE TO LOOK
| Task | Location | Notes |
|------|----------|-------|
| Define Kafka clients/listeners | annotation/ | Runtime + APT usage patterns |
| Configure producers/consumers | config/ | Maps Kafka props → Micronaut beans |
| Message binding | bind/ | Argument/Body binders for Kafka records |
| Metrics | metrics/ | Micrometer registry wiring |
| Events | event/ | AbstractKafkaApplicationEvent + concrete consumer events |
| Lifecycle/processing | processor/ | Factories, group mgmt, internal listeners |
| Seek ops | seek/ | Programmatic seeking, helpers |
| Serde | serde/ | Registry, custom serde patterns |
| Example tests | src/test/groovy/... | Testcontainers Kafka; behavior coverage |

## CONVENTIONS (module-specific)
- Events pattern: AbstractKafkaApplicationEvent<T> base; final event types for consumer lifecycle stages.
- Tests: Groovy/Spock with @MicronautTest; Testcontainers Kafka via BOM-managed deps.
- Annotations drive bean creation; factories under processor/ wire clients/listeners.

## ANTI-PATTERNS (module-specific)
- Don’t bypass binders with manual header/value extraction unless necessary.
- Don’t emit metrics directly; use provided metrics integration.
- Don’t couple tests to broker ports; rely on Testcontainers.
