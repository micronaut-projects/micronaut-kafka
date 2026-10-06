package io.micronaut.configuration.kafka.processor.reload

import io.micronaut.configuration.kafka.ConsumerRegistry
import io.micronaut.configuration.kafka.KafkaConsumerFactory
import io.micronaut.configuration.kafka.ProducerRegistry
import io.micronaut.configuration.kafka.annotation.KafkaClient
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.OffsetReset
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.configuration.kafka.config.AbstractKafkaConsumerConfiguration
import io.micronaut.configuration.kafka.serde.SerdeRegistry
import io.micronaut.context.ApplicationContext
import io.micronaut.context.DefaultBeanContext
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Parameter
import io.micronaut.context.annotation.Prototype
import io.micronaut.context.annotation.Replaces
import io.micronaut.context.annotation.Requires
import io.micronaut.context.reload.ClassChange
import io.micronaut.context.reload.ClassChangeEvent
import io.micronaut.context.reload.ReloadStrategy
import io.micronaut.inject.BeanDefinition
import io.micronaut.inject.qualifiers.Qualifiers
import jakarta.inject.Named
import jakarta.inject.Singleton
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.MockConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.Serializer
import spock.lang.Specification
import spock.util.concurrent.PollingConditions

import java.time.Duration
import java.util.concurrent.CopyOnWriteArrayList

/**
 * The development reloader over consumers that are {@link MockConsumer}s: no broker is needed.
 */
class KafkaReloadSpec extends Specification {

    private static final String RELOADER = 'io.micronaut.configuration.kafka.processor.DevelopmentKafkaReloader'
    private static final String TOPIC = 'reload-spec-topic'

    PollingConditions conditions = new PollingConditions(timeout: 10)

    void setup() {
        MockConsumers.created.clear()
    }

    void "in development mode an in-place change of a listener class restarts the consumers on a new processor and a new listener, and a restart or an unrelated change does not"() {
        given:
        ApplicationContext context = devContext(true)
        ReloadListener listener = context.getBean(ReloadListener)
        ConsumerRegistry registry = context.getBean(ConsumerRegistry)
        MockConsumer<?, ?> first = polling()

        expect: 'the reloader exists only in development mode'
        context.containsBean(reloader())
        first.subscription() == [TOPIC] as Set
        consume(first, 'one')
        conditions.eventually { assert listener.received == ['one'] }

        when: 'the application restarts: the new context starts its own consumers'
        context.publishEvent(classChange([ReloadListener.classLoader] as Set, [], ReloadStrategy.RESTART))

        then:
        !first.closed()
        context.getBean(ConsumerRegistry).is(registry)
        MockConsumers.mine().size() == 1

        when: 'a class that is not a listener is redefined in place'
        context.publishEvent(classChange([] as Set, [new ClassChange(KafkaReloadSpec.name, ClassChange.Kind.MODIFIED)], ReloadStrategy.RELOAD))

        then:
        !first.closed()
        context.getBean(ConsumerRegistry).is(registry)
        context.getBean(ReloadListener).is(listener)

        when: 'the listener class is redefined in place'
        context.publishEvent(classChange([] as Set, [new ClassChange(ReloadListener.name, ClassChange.Kind.MODIFIED)], ReloadStrategy.RELOAD))
        ReloadListener recreated = context.getBean(ReloadListener)

        then: 'the consumer of the replaced code is closed, and it left the group as it closed'
        closed(first)

        and: 'the processor and the listener are new, and a new consumer subscribes for the new listener'
        !context.getBean(ConsumerRegistry).is(registry)
        !recreated.is(listener)
        !context.getBean(ConsumerRegistry).consumerIds.isEmpty()
        MockConsumer<?, ?> second = polling()
        !second.is(first)
        second.subscription() == [TOPIC] as Set

        and: 'records reach the new listener only'
        consume(second, 'two')
        conditions.eventually { assert recreated.received == ['two'] }
        listener.received == ['one']

        cleanup:
        context.close()
    }

    void "in development mode a serde change or a retired classloader recreates the serde and producer registries, the Kafka clients, and restarts the consumers"() {
        given:
        ApplicationContext context = devContext(true)
        ReloadClient client = context.getBean(ReloadClient)
        SerdeRegistry serdes = context.getBean(SerdeRegistry)
        ProducerRegistry producers = context.getBean(ProducerRegistry)
        ConsumerRegistry registry = context.getBean(ConsumerRegistry)
        ReloadListener listener = context.getBean(ReloadListener)
        MockConsumer<?, ?> first = polling()

        when: 'a serde is redefined in place'
        context.publishEvent(classChange([] as Set, [new ClassChange(ReloadSerializer.name, ClassChange.Kind.MODIFIED)], ReloadStrategy.RELOAD))

        then: 'the registries keyed by class are new, and so are the beans that received them'
        !context.getBean(SerdeRegistry).is(serdes)
        !context.getBean(ProducerRegistry).is(producers)
        !context.getBean(ReloadClient).is(client)
        !context.getBean(ConsumerRegistry).is(registry)

        and: 'the consumers are started again, the listener bean being unchanged'
        closed(first)
        MockConsumer<?, ?> second = polling()
        !second.is(first)
        context.getBean(ReloadListener).is(listener)

        when: 'a reload retires a classloader'
        serdes = context.getBean(SerdeRegistry)
        registry = context.getBean(ConsumerRegistry)
        context.publishEvent(classChange([ReloadListener.classLoader] as Set, [], ReloadStrategy.RELOAD))

        then:
        closed(second)
        !context.getBean(SerdeRegistry).is(serdes)
        !context.getBean(ConsumerRegistry).is(registry)
        MockConsumer<?, ?> third = polling()
        !third.is(second)

        cleanup:
        context.close()
    }

    void "in development mode a serde definition registered or removed while running recreates the serde and producer registries"() {
        given:
        ApplicationContext context = devContext(true)
        SerdeRegistry serdes = context.getBean(SerdeRegistry)
        ProducerRegistry producers = context.getBean(ProducerRegistry)
        ReloadClient client = context.getBean(ReloadClient)
        MockConsumer<?, ?> first = polling()
        BeanDefinition<?> serializer = context.getBeanDefinition(Serializer, Qualifiers.byName('reload-spec'))

        when: 'the launcher registers a serializer definition'
        ((DefaultBeanContext) context).notifyDefinitionChange([], [serializer])

        then: 'the registries keyed by class are new, and so are the beans that received them'
        !context.getBean(SerdeRegistry).is(serdes)
        !context.getBean(ProducerRegistry).is(producers)
        !context.getBean(ReloadClient).is(client)
        closed(first)
        MockConsumer<?, ?> second = polling()

        when: 'the launcher removes it'
        serdes = context.getBean(SerdeRegistry)
        producers = context.getBean(ProducerRegistry)
        ((DefaultBeanContext) context).notifyDefinitionChange([serializer], [])

        then:
        !context.getBean(SerdeRegistry).is(serdes)
        !context.getBean(ProducerRegistry).is(producers)
        closed(second)
        polling() != second

        cleanup:
        context.close()
    }

    void "in development mode an in-place change of a factory that produces a serde recreates the serde and producer registries, and of a factory of other beans does not"() {
        given:
        ApplicationContext context = devContext(true)
        SerdeRegistry serdes = context.getBean(SerdeRegistry)
        ProducerRegistry producers = context.getBean(ProducerRegistry)
        MockConsumer<?, ?> first = polling()

        when: 'a factory of consumers is redefined in place'
        context.publishEvent(classChange([] as Set, [new ClassChange(MockConsumerFactory.name, ClassChange.Kind.MODIFIED)], ReloadStrategy.RELOAD))

        then:
        context.getBean(SerdeRegistry).is(serdes)
        context.getBean(ProducerRegistry).is(producers)
        !first.closed()

        when: 'the factory of a serializer is redefined in place'
        context.publishEvent(classChange([] as Set, [new ClassChange(ReloadSerdeFactory.name, ClassChange.Kind.MODIFIED)], ReloadStrategy.RELOAD))

        then:
        !context.getBean(SerdeRegistry).is(serdes)
        !context.getBean(ProducerRegistry).is(producers)
        closed(first)
        polling() != first

        cleanup:
        context.close()
    }

    void "in development mode a listener definition registered while running restarts the consumers once"() {
        given:
        ApplicationContext context = devContext(true)
        MockConsumer<?, ?> first = polling()
        BeanDefinition<?> definition = context.getBeanDefinition(ReloadListener)

        when: 'the launcher swaps the definition of the listener for another of the same class'
        ((DefaultBeanContext) context).notifyDefinitionChange([definition], [definition])

        then: 'every consumer started before is closed, and exactly one consumer polls for the listener'
        conditions.eventually {
            assert MockConsumers.mine().findAll { !it.closed() }.size() == 1
        }
        closed(first)
        !context.getBean(ConsumerRegistry).consumerIds.isEmpty()

        cleanup:
        context.close()
    }

    void "in development mode the context starts the consumers again on a new processor when another module recreates a bean the processor received, without the reloader"() {
        given:
        ApplicationContext context = devContext(true)
        ConsumerRegistry registry = context.getBean(ConsumerRegistry)
        ReloadListener listener = context.getBean(ReloadListener)
        MockConsumer<?, ?> first = polling()

        when: 'a module recreates the serde registry for a change of its own, which destroys the processor'
        context.recreate(context.getBean(SerdeRegistry))

        then: 'the context created the processor again at once, and gave it the listener methods: no class change follows'
        closed(first)
        !context.getBean(ConsumerRegistry).is(registry)
        MockConsumer<?, ?> second = polling()
        !second.is(first)
        second.subscription() == [TOPIC] as Set

        and: 'records reach the listener, which was not recreated'
        consume(second, 'two')
        conditions.eventually { assert listener.received == ['two'] }

        when: 'the processor itself is recreated'
        context.recreate(context.getBean(ConsumerRegistry))

        then: 'exactly one consumer polls on the new processor'
        closed(second)
        polling() != second

        cleanup:
        context.close()
    }

    void "in development mode a context that does not track bean dependencies keeps the consumers running"() {
        given:
        ApplicationContext context = devContext(false)
        ConsumerRegistry registry = context.getBean(ConsumerRegistry)
        MockConsumer<?, ?> first = polling()

        expect:
        context.containsBean(reloader())

        when:
        context.publishEvent(classChange([] as Set, [new ClassChange(ReloadListener.name, ClassChange.Kind.MODIFIED)], ReloadStrategy.RELOAD))

        then:
        !first.closed()
        context.getBean(ConsumerRegistry).is(registry)
        MockConsumers.mine().size() == 1

        cleanup:
        context.close()
    }

    void "outside development mode there is no reloader, and the same beans keep their consumers through class changes"() {
        given:
        ApplicationContext context = ApplicationContext.run(kafkaProperties())
        ConsumerRegistry registry = context.getBean(ConsumerRegistry)
        SerdeRegistry serdes = context.getBean(SerdeRegistry)
        ReloadListener listener = context.getBean(ReloadListener)
        MockConsumer<?, ?> first = polling()

        expect:
        !context.containsBean(reloader())

        when:
        context.publishEvent(classChange([ReloadListener.classLoader] as Set, [new ClassChange(ReloadListener.name, ClassChange.Kind.MODIFIED)], ReloadStrategy.RELOAD))

        then:
        !first.closed()
        context.getBean(ConsumerRegistry).is(registry)
        context.getBean(SerdeRegistry).is(serdes)
        context.getBean(ReloadListener).is(listener)
        MockConsumers.mine().size() == 1

        and: 'the listener consumes as before'
        consume(first, 'one')
        conditions.eventually { assert listener.received == ['one'] }

        cleanup:
        context.close()
    }

    // a consumer closes as its poll loop ends, which a consumer that never polled may reach after the processor is destroyed
    private boolean closed(MockConsumer<?, ?> consumer) {
        conditions.eventually { assert consumer.closed() }
        return true
    }

    private PollingMockConsumer<?, ?> polling() {
        conditions.eventually {
            List<PollingMockConsumer> open = MockConsumers.mine().findAll { !it.closed() }
            assert open.size() == 1
            assert open[0].polled
        }
        return MockConsumers.mine().find { !it.closed() }
    }

    private static boolean consume(MockConsumer<?, ?> consumer, String value) {
        TopicPartition partition = new TopicPartition(TOPIC, 0)
        consumer.schedulePollTask {
            consumer.rebalance([partition])
            consumer.updateBeginningOffsets([(partition): 0L])
            consumer.addRecord(new ConsumerRecord(TOPIC, 0, 0L, null, value))
        }
        return true
    }

    private static ApplicationContext devContext(boolean track) {
        return ApplicationContext.builder()
            .properties(kafkaProperties() + ['micronaut.dev.enabled': true])
            .trackBeanDependencies(track)
            .start()
    }

    private static Map<String, Object> kafkaProperties() {
        return ['spec.name': 'KafkaReloadSpec', 'kafka.bootstrap.servers': 'localhost:9', 'kafka.health.enabled': false]
    }

    private static Class<?> reloader() {
        return Class.forName(RELOADER)
    }

    private static ClassChangeEvent classChange(Set<ClassLoader> retired, List<ClassChange> changes, ReloadStrategy strategy) {
        return new ClassChangeEvent(KafkaReloadSpec, 1, retired, KafkaReloadSpec.classLoader, changes, strategy)
    }
}

class MockConsumers {
    static final List<PollingMockConsumer> created = new CopyOnWriteArrayList<>()

    // the consumers of the listener of the spec: the other listeners of the test sources are started too
    static List<PollingMockConsumer> mine() {
        return created.findAll { it.topics.contains('reload-spec-topic') }
    }
}

class PollingMockConsumer<K, V> extends MockConsumer<K, V> {
    volatile boolean polled
    final Set<String> topics = java.util.concurrent.ConcurrentHashMap.newKeySet()

    PollingMockConsumer() {
        super('earliest')
    }

    @Override
    synchronized void subscribe(Collection<String> subscribed) {
        topics.addAll(subscribed)
        super.subscribe(subscribed)
    }

    @Override
    synchronized ConsumerRecords<K, V> poll(Duration timeout) {
        polled = true
        // a broker would block for the timeout: the waiting gives the lock up, so the test can schedule records
        wait(20)
        return super.poll(timeout)
    }
}

@Factory
@Requires(property = 'spec.name', value = 'KafkaReloadSpec')
class MockConsumerFactory {
    @Prototype
    @Replaces(factory = KafkaConsumerFactory, value = Consumer)
    <K, V> Consumer<K, V> createConsumer(@Parameter AbstractKafkaConsumerConfiguration<K, V> configuration) {
        PollingMockConsumer<K, V> consumer = new PollingMockConsumer<>()
        MockConsumers.created.add(consumer)
        return consumer
    }
}

@KafkaListener(offsetReset = OffsetReset.EARLIEST, groupId = 'reload-spec')
@Requires(property = 'spec.name', value = 'KafkaReloadSpec')
class ReloadListener {
    final List<String> received = new CopyOnWriteArrayList<>()

    @Topic('reload-spec-topic')
    void receive(String value) {
        received.add(value)
    }
}

@KafkaClient
@Requires(property = 'spec.name', value = 'KafkaReloadSpec')
interface ReloadClient {
    @Topic('reload-spec-topic')
    void send(String value)
}

class ReloadSerializer implements org.apache.kafka.common.serialization.Serializer<String> {
    @Override
    byte[] serialize(String topic, String data) {
        return data?.bytes
    }
}

@Factory
@Requires(property = 'spec.name', value = 'KafkaReloadSpec')
class ReloadSerdeFactory {
    @Singleton
    @Named('reload-spec')
    Serializer<String> serializer() {
        return new ReloadSerializer()
    }
}
