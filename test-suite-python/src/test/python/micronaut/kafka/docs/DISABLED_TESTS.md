# Python Docs Disabled Test Inventory

This file tracks Python docs examples of Micronaut Kafka that are present but disabled, or that deviate from the
Java example because the direct port currently fails compilation or at runtime. Use it as the bug-fixing task list
for the final migration wave.

## Reconciliation

- Last generated active `@Disabled` count: 1.
- Last generated command: `rg -n "@Disabled\\(" test-suite-python/src/test/python`.
- Last full-suite command: `./gradlew :test-suite-python:test -Ppython-ci` (needs a container runtime for the Kafka test container).
- Last full-suite result (micronaut-core 5.2.3, micronaut-build 8.1.2): build successful, 13 tests executed, 1 skipped (`MyTest`, see below), 0 failures.

## Migration Rules

- Do not define local copies of Micronaut annotation helpers or custom annotation shims in docs snippets. Standard
  Micronaut and Kafka annotations are generated from imports (`from micronaut.configuration.kafka.annotation import
  KafkaClient, KafkaListener, KafkaKey, Topic, OffsetReset, ...`).
- `@KafkaClient` interfaces are abstract classes (`ABC`) whose abstract methods have `...` bodies; `@KafkaListener` beans
  are plain classes with `@Topic` methods. Parameter annotations use `Annotated[str, KafkaKey]`.
- Do not add Java-style getters or setters to Python docs models. Prefer `@Serdeable @dataclass(frozen=True)` models.
- Methods that implement or override a Java interface keep the Java (camelCase) name; other methods are snake_case.
- Java classes are imported like Python modules (`from reactor.core.publisher import Flux, Mono`, `from java.lang import
  String`); nested types are attributes of the imported outer class like in the Java examples (`KafkaClient.Acknowledge.ALL`,
  `KafkaMessage.Builder`, `KafkaStreams.State`, `ConditionalRetryBehaviourHandler.ConditionalRetryBehaviour`,
  `StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse`); importing a nested type directly
  (`from micronaut.configuration.kafka.annotation.KafkaClient import Acknowledge`) works as well (`ProductClientTest`).
  No snippet uses `java.type(...)`.
- Logging uses Python's `logging` module (`LOG = logging.getLogger(__name__)`), never slf4j.
- Method names that are Python keywords use the generated keyword-safe alias: `Materialized.as_(...)`,
  `Grouped.with_(...)`, `Produced.with_(...)`.
- Python `int` is Java `int`; 64-bit listener arguments (`offset`, `timestamp`, Kafka Streams `count()` values) are
  declared as `java.lang.Long`; `bytes` is `byte[]`.
- Prefer `@MicronautTest(environments=["kafka"])` with injected beans over `ApplicationContext.run()`. The
  `kafka.bootstrap.servers` of the shared test container is supplied by the Java
  `io.micronaut.kafka.docs.KafkaTestConfigurer` (`@ContextConfigurer`) of this project (see below).

## Active `@Disabled` Tests

| Test | Reason |
| --- | --- |
| `io.micronaut.kafka.docs.MyTest` | `TestPropertyProvider.getProperties()` is called by Micronaut Test before the application context, and with it the GraalPy runtime, exists (`java.lang.IllegalStateException: GraalPy context has not been initialized. Make sure micronaut-context-python is on the classpath.` from `PythonContextRuntime.newInstance` in the generated `MyTest.getProperties()`), so a Python test class cannot provide the container's bootstrap servers. The test now extends the Python `AbstractKafkaTest` like the Java, Kotlin and Groovy versions (the stub compiles with core 5.2.3). |

## Commented Unsupported Snippet Ports

| Target | Reason |
| --- | --- |
| `io.micronaut.kafka.docs.consumer.batch.BookClient` (`arrays` tag) | Variadic parameters (`Book... books`) are not mapped to Java arrays by the Python compiler (`def send_books(self, *books: Book)` generates a method without parameters); the `arrays` tag only holds a comment pointing at the list variant. |

## Workarounds Kept In Snippets

| Target | Reason |
| --- | --- |
| `io.micronaut.kafka.docs.quickstart.RuntimeBootstrapServers` | The snippet compiles and is registered as an `ApplicationContextConfigurer` service (both `configure` overloads are bridged with core 5.2.3), but like in the Java suite the test runtime classpath excludes its configurer metadata: it points at `localhost:9092` and would run before the GraalPy runtime exists. |
| `io.micronaut.kafka.docs.streams.WordCountStream` | `KStream.flatMapValues(lambda)` cannot be called from Python: the `ValueMapper` and `ValueMapperWithKey` overloads are ambiguous for a Python function (still with core 5.2.3: `BeanInstantiationException: Error instantiating bean of type [org.apache.kafka.streams.KafkaStreams] Message: TypeError: invalid instantiation of foreign object`, raised while the `@EachBean KafkaStreams` factory builds the topology), so the Python stream uses `flatMap` with a `KeyValue`-returning lambda instead. `TODO(python)`. |
| `io.micronaut.kafka.docs.consumer.scope.ProductListener` | The `@KafkaScope` (`@ScopedProxy`) Python bean `ProductMetadata` is resolved through the custom scope when it is injected into the listener (`BeanInstantiationException: Error instantiating bean of type [micronaut.kafka.docs.consumer.scope.ProductListener] Message: No active Kafka scope`, path `new ProductListener(ProductMetadata product_metadata)`) instead of through a scoped proxy (the Python compiler generates a `$ProductMetadata$RuntimeProxy$Definition` but no `$Intercepted` scoped proxy like javac does), so the listener (which has no `@Requires` in Java) is restricted to `spec.name=KafkaScopeListenerTest` in Python to keep it out of the other Kafka tests. Still present with core 5.2.3. `TODO(python)`. |
| `io.micronaut.kafka.docs.streams.KafkaTestInitializer` | A Python `BootstrapPropertySourceLocator` bean cannot be created in the bootstrap context (no GraalPy runtime yet); the Python initializer creates the input topics from a `@EventListener` for `BeforeKafkaStreamStart` instead. |
| `io.micronaut.kafka.docs.KafkaTestConfigurer` (Java, `src/test/java`) | The `@ContextConfigurer` providing `kafka.bootstrap.servers` of the shared Kafka test container is written in Java because Micronaut Test calls `TestPropertyProvider` before the application context, and with it the GraalPy runtime, exists. It implements `configure(ApplicationContext)` (the builder overload runs before `@MicronautTest` applies its environments) and adds the property source only when the `kafka` environment is active; Python tests select it with `@MicronautTest(environments=["kafka"])`. The test runtime classpath only excludes the configurer metadata of the `RuntimeBootstrapServers` snippet (`build.gradle.kts`). |

## `java.type` usages

None.

## Verified with micronaut-core 5.2.3 (workarounds removed)

- `@KafkaListener` on listener *methods* (`consumer.batch.ack.BookListener`, `consumer.batch.manual.BookListener`, both
  methods in one class like in Java).
- Reactive listener methods return `Mono[...]`/`Flux[...]` (`consumer.reactive.ProductListener`,
  `consumer.sendto.ProductListener`, `consumer.batch.BookListener`).
- `acks=KafkaClient.Acknowledge.ALL` records the `Acknowledge.ALL` constant (`-1`, `Acknowledge` is a constants class)
  and nested types can be imported directly (`ProductClientTest`).
- `getBean(PythonClass)` returns the Python object (no `asPolyglotValue()` calls in the tests).
- The GraalPy runtime is created on demand for beans instantiated by `processOnStartup` processors
  (`PythonRuntimeInitializer` removed).
- Python overrides of default interface methods (`ConsumerSeekAware.onPartitionsRevoked`,
  `ApplicationContextConfigurer.configure`) are bridged.
- `class MyTest(AbstractKafkaTest)` compiles (the test stays disabled for the `TestPropertyProvider` timing above).

## Intentionally Unsupported Snippet Targets

None.
