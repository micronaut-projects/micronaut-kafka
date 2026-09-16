# Python Docs Disabled Test Inventory

This file tracks Python docs examples of Micronaut Kafka that are present but disabled, or that deviate from the
Java example because the direct port currently fails compilation or at runtime. Use it as the bug-fixing task list
for the final migration wave.

## Reconciliation

- Last generated active `@Disabled` count: 1.
- Last generated command: `rg -n "@Disabled\\(" test-suite-python/src/test/python`.
- Last full-suite command: `./gradlew :test-suite-python:test -Ppython-ci` (needs a container runtime for the Kafka test container).
- Last full-suite result: build successful, 12 tests executed, 1 skipped (`MyTest`, see below), 0 failures.

## Migration Rules

- Do not define local copies of Micronaut annotation helpers or custom annotation shims in docs snippets. Standard
  Micronaut and Kafka annotations are generated from imports (`from micronaut.configuration.kafka.annotation import
  KafkaClient, KafkaListener, KafkaKey, Topic, OffsetReset, ...`).
- `@KafkaClient` interfaces are abstract classes (`ABC`) whose abstract methods have `...` bodies; `@KafkaListener` beans
  are plain classes with `@Topic` methods. Parameter annotations use `Annotated[str, KafkaKey]`.
- Do not add Java-style getters or setters to Python docs models. Prefer `@Serdeable @dataclass(frozen=True)` models.
- Methods that implement or override a Java interface keep the Java (camelCase) name; other methods are snake_case.
- Java classes are imported like Python modules (`from reactor.core.publisher import Flux, Mono`, `from java.lang import
  String`); nested types are attributes of the imported outer class (`KafkaClient.Acknowledge.ALL`, `KafkaMessage.Builder`,
  `KafkaStreams.State`, `ConditionalRetryBehaviourHandler.ConditionalRetryBehaviour`,
  `StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse`). No snippet uses `java.type(...)`.
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
| `io.micronaut.kafka.docs.MyTest` | `TestPropertyProvider.getProperties()` is called by Micronaut Test before the application context, and with it the GraalPy runtime, exists (`GraalPy context has not been initialized`), so a Python test class cannot provide the container's bootstrap servers. A Python test class cannot extend the Python `AbstractKafkaTest` either (`Failed to generate stub for Python type [MyTest]: Expected graalpyInternalValue field`), so it implements `TestPropertyProvider` directly. |

## Commented Unsupported Snippet Ports

| Target | Reason |
| --- | --- |
| `io.micronaut.kafka.docs.consumer.batch.BookClient` (`arrays` tag) | Variadic parameters (`Book... books`) are not mapped to Java arrays by the Python compiler (`def send_books(self, *books: Book)` generates a method without parameters); the `arrays` tag only holds a comment pointing at the list variant. |

## Workarounds Kept In Snippets

| Target | Reason |
| --- | --- |
| `io.micronaut.kafka.docs.consumer.batch.ack.BookListener`, `io.micronaut.kafka.docs.consumer.batch.manual.BookListener` | A method decorated with `@KafkaListener` (a `@Bean`-stereotyped annotation) is treated as a factory method by the Python compiler (`Factory methods declared with @Bean must specify a return type`), so `@KafkaListener` is declared on the class; the second listener method of the manual example lives in its own class (`BookRecordsListener`). |
| `io.micronaut.kafka.docs.quickstart.RuntimeBootstrapServers` | The snippet compiles and is registered as an `ApplicationContextConfigurer` service, but it cannot run: `ApplicationContextConfigurer.configure` is a default interface method, and Python overrides of default interface methods are not bridged to Java; the configurer would also run before the GraalPy runtime exists. Like the Java suite, the test runtime classpath excludes the configurer metadata. |
| `io.micronaut.kafka.docs.consumer.reactive.ProductListener`, `io.micronaut.kafka.docs.consumer.sendto.ProductListener` (`reactive` tag), `io.micronaut.kafka.docs.consumer.batch.BookListener` (`reactive` tag) | A bridged method declared to return `Mono[...]`/`Flux[...]` wraps the Python result with `Publishers.map(...)` and casts it to `Mono`/`Flux` (`ClassCastException: Publishers$$Lambda cannot be cast to reactor.core.publisher.Mono`), so the reactive listener methods are declared to return `Publisher[...]` (the `Mono`/`Flux` parameters are unaffected). |
| `io.micronaut.kafka.docs.streams.WordCountStream` | `KStream.flatMapValues(lambda)` cannot be called from Python: the `ValueMapper` and `ValueMapperWithKey` overloads are ambiguous for a Python function (`TypeError: invalid instantiation of foreign object`), so the Python stream uses `flatMap` with a `KeyValue`-returning lambda instead. |
| `io.micronaut.kafka.docs.seek.aware.ProductListener` | `ConsumerSeekAware.onPartitionsRevoked` is a default interface method, so the (empty) Python override is never invoked from Java. |
| `io.micronaut.kafka.docs.consumer.scope.ProductListener` | The `@KafkaScope` (`@ScopedProxy`) Python bean `ProductMetadata` is resolved through the custom scope when it is injected into the listener (`No active Kafka scope`) instead of through the lazy runtime proxy, so the listener (which has no `@Requires` in Java) is restricted to `spec.name=KafkaScopeListenerTest` in Python to keep it out of the other Kafka tests. |
| `io.micronaut.kafka.docs.PythonRuntimeInitializer` (Java, `src/test/java`) | `@Executable(processOnStartup = true)` processors such as the Kafka consumer processor run before `@Context` beans are initialized, so a Python `@KafkaListener` bean is instantiated before the GraalPy runtime exists (`GraalPy context has not been initialized`); `PythonRuntimeInitializer` creates the GraalPy context bean when an `ExecutableMethodProcessor` is created. |
| `io.micronaut.kafka.docs.streams.KafkaTestInitializer` | A Python `BootstrapPropertySourceLocator` bean cannot be created in the bootstrap context (no GraalPy runtime yet); the Python initializer creates the input topics from a `@EventListener` for `BeforeKafkaStreamStart` instead. |
| `io.micronaut.kafka.docs.KafkaTestConfigurer` (Java, `src/test/java`) | The `@ContextConfigurer` providing `kafka.bootstrap.servers` of the shared Kafka test container is written in Java because Micronaut Test calls `TestPropertyProvider` before the application context, and with it the GraalPy runtime, exists. It implements `configure(ApplicationContext)` (the builder overload runs before `@MicronautTest` applies its environments) and adds the property source only when the `kafka` environment is active; Python tests select it with `@MicronautTest(environments=["kafka"])`. The test runtime classpath only excludes the configurer metadata of the `RuntimeBootstrapServers` snippet (`build.gradle.kts`). |
| `io.micronaut.kafka.docs.producer.config.ProductClient` | `acks=KafkaClient.Acknowledge.ALL` (a nested enum constant as annotation member) is recorded by the Python compiler as the string `io.micronaut.configuration.kafka.annotation.KafkaClient.Acknowledge.ALL` instead of the constant name `ALL` (a `java.type` alias recorded the enum's `-1` value instead), so the `acks` member would fall back to its default at runtime; top-level enum constants (`OffsetReset.EARLIEST`) are recorded correctly. The snippet is not executed by a test. `TODO(python)`. |

## `java.type` usages

None. `from a.b.Outer import Inner` must not be used for nested types: the compiler generates an `Outer/__init__.py`
package for the nested import that shadows the `Outer.py` shim (`ImportError: cannot import name 'KafkaClient' from
'micronaut.configuration.kafka.annotation.KafkaClient'`) or rebinds the `Outer` name of the package `__init__.py` to the
sub-module (`from . import KafkaStreams` after `KafkaStreams = java.type(...)`). Nested types are accessed as attributes of
the outer class instead (`KafkaStreams.State`), which GraalPy host interop supports. `TODO(python)`.

## Intentionally Unsupported Snippet Targets

None.
