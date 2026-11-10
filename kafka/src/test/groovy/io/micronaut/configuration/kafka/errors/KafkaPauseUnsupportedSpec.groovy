package io.micronaut.configuration.kafka.errors

import io.micronaut.configuration.kafka.AbstractKafkaContainerSpec
import io.micronaut.configuration.kafka.ConsumerFactory
import io.micronaut.configuration.kafka.ConsumerRegistry
import io.micronaut.configuration.kafka.DefaultConsumerFactory
import io.micronaut.configuration.kafka.annotation.ErrorStrategy
import io.micronaut.configuration.kafka.annotation.KafkaClient
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.KafkaPartition
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.context.annotation.Property
import io.micronaut.context.annotation.Replaces
import io.micronaut.context.annotation.Requires
import jakarta.inject.Singleton
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.Deserializer
import org.jspecify.annotations.Nullable

import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Proxy
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

import static io.micronaut.configuration.kafka.annotation.ErrorStrategyValue.RETRY_ON_ERROR
import static io.micronaut.configuration.kafka.annotation.OffsetReset.EARLIEST
import static io.micronaut.configuration.kafka.annotation.OffsetStrategy.SYNC

/**
 * Consumers that cannot pause, like Oracle's OKafka consumer, stop polling while a partition is paused instead.
 */
class KafkaPauseUnsupportedSpec extends AbstractKafkaContainerSpec {

    @Override
    void startContext() {
        // the group listener would otherwise create the topic with a single partition when it starts
        bootstrapServers = kafkaContainer.bootstrapServers
        createTopic('pause-unsupported-group', 2, 1)
        super.startContext()
    }

    void "a #mode consumer that cannot pause holds a record whose retries are exhausted until it is resumed"() {
        given:
        UnsupportedPauseClient client = context.getBean(UnsupportedPauseClient)
        ConsumerRegistry registry = context.getBean(ConsumerRegistry)
        def listener = context.getBean(listenerType)

        when:
        client.send(topic, 'ERROR')

        then: "the record is tried once and retried twice, then its partition is paused"
        conditions.eventually {
            listener.received == ['ERROR', 'ERROR', 'ERROR']
            registry.isPaused(topic)
        }

        when: "another record is sent and more time passes than many retry delays"
        client.send(topic, 'OK')
        sleep(2000)

        then: "neither record is passed to the listener again"
        listener.received == ['ERROR', 'ERROR', 'ERROR']
        registry.isPaused(topic)

        when: "the listener is allowed to succeed and the partition is resumed"
        listener.fail.set(false)
        registry.resume(topic)

        then: "consumption continues from the failed record"
        conditions.eventually {
            !registry.isPaused(topic)
            listener.successful == ['ERROR', 'OK']
        }

        where:
        mode     | topic                           | listenerType
        'single' | 'pause-unsupported-stop-single' | StopOnExhaustedSingleListener
        'batch'  | 'pause-unsupported-stop-batch'  | StopOnExhaustedBatchListener
    }

    void "a record whose retries are exhausted does not stop the other consumers in the group"() {
        given:
        UnsupportedPauseClient client = context.getBean(UnsupportedPauseClient)
        GroupListener listener = context.getBean(GroupListener)

        when: "a record keeps failing on partition 0"
        client.send('pause-unsupported-group', 0, 'ERROR')

        then:
        conditions.eventually {
            listener.received.count('ERROR') == 3
        }

        when: "more time passes than many retry delays, and a record is sent to partition 1"
        Map<String, AtomicInteger> polls = context.getBean(UnsupportedPauseConsumerFactory).polls
        int pollsBefore = groupPolls(polls)
        sleep(1000)
        int pollsDuring = groupPolls(polls) - pollsBefore
        client.send('pause-unsupported-group', 1, 'OK')

        then: "the failed record is not retried by another consumer, and partition 1 keeps being consumed"
        conditions.eventually {
            listener.received.contains('OK')
        }
        listener.received.count('ERROR') == 3

        and: "the consumer holding the failed record does not poll, and the other one polls as usual"
        pollsDuring <= 15
    }

    private static int groupPolls(Map<String, AtomicInteger> polls) {
        polls.findAll { it.key.startsWith('pause-unsupported-group') }.values().sum { it.get() } as int
    }

    void "the registry pauses and resumes a consumer that cannot pause"() {
        given:
        UnsupportedPauseClient client = context.getBean(UnsupportedPauseClient)
        ConsumerRegistry registry = context.getBean(ConsumerRegistry)
        RegistryListener listener = context.getBean(RegistryListener)
        conditions.eventually {
            !registry.getConsumerAssignment('pause-unsupported-registry').empty
        }

        when:
        registry.pause('pause-unsupported-registry')

        then:
        conditions.eventually {
            registry.isPaused('pause-unsupported-registry')
        }

        when:
        client.send('pause-unsupported-registry', 'one')
        sleep(500)
        AtomicInteger polls = context.getBean(UnsupportedPauseConsumerFactory).polls['pause-unsupported-registry']
        int pollsBefore = polls.get()
        sleep(2000)

        then: "the record is not received, because the consumer does not poll"
        listener.received.empty
        polls.get() == pollsBefore

        when:
        registry.resume('pause-unsupported-registry')

        then:
        conditions.eventually {
            !registry.isPaused('pause-unsupported-registry')
            listener.received == ['one']
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaPauseUnsupportedSpec')
    @KafkaClient
    static interface UnsupportedPauseClient {
        void send(@Topic String topic, String message)

        void send(@Topic String topic, @KafkaPartition int partition, String message)
    }

    @Requires(property = 'spec.name', value = 'KafkaPauseUnsupportedSpec')
    @KafkaListener(
        clientId = 'pause-unsupported-stop-single',
        offsetReset = EARLIEST,
        offsetStrategy = SYNC,
        uniqueGroupId = true,
        errorStrategy = @ErrorStrategy(value = RETRY_ON_ERROR, retryCount = 2, retryDelay = "50ms", stopOnExhaustedRetry = true),
        properties = @Property(name = ConsumerConfig.MAX_POLL_RECORDS_CONFIG, value = "1")
    )
    static class StopOnExhaustedSingleListener {
        final AtomicBoolean fail = new AtomicBoolean(true)
        final List<String> received = new CopyOnWriteArrayList<>()
        final List<String> successful = new CopyOnWriteArrayList<>()

        @Topic('pause-unsupported-stop-single')
        void receive(String message) {
            received << message
            if (fail.get()) {
                throw new RuntimeException('Still failing')
            }
            successful << message
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaPauseUnsupportedSpec')
    @KafkaListener(
        clientId = 'pause-unsupported-stop-batch',
        batch = true,
        offsetReset = EARLIEST,
        offsetStrategy = SYNC,
        uniqueGroupId = true,
        errorStrategy = @ErrorStrategy(value = RETRY_ON_ERROR, retryCount = 2, retryDelay = "50ms", stopOnExhaustedRetry = true)
    )
    static class StopOnExhaustedBatchListener {
        final AtomicBoolean fail = new AtomicBoolean(true)
        final List<String> received = new CopyOnWriteArrayList<>()
        final List<String> successful = new CopyOnWriteArrayList<>()

        @Topic('pause-unsupported-stop-batch')
        void receive(List<String> messages) {
            received.addAll(messages)
            if (fail.get()) {
                throw new RuntimeException('Still failing')
            }
            successful.addAll(messages)
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaPauseUnsupportedSpec')
    @KafkaListener(
        groupId = 'pause-unsupported-group',
        clientId = 'pause-unsupported-group',
        threads = 2,
        offsetReset = EARLIEST,
        offsetStrategy = SYNC,
        errorStrategy = @ErrorStrategy(value = RETRY_ON_ERROR, retryCount = 2, retryDelay = "50ms", stopOnExhaustedRetry = true),
        properties = @Property(name = ConsumerConfig.MAX_POLL_RECORDS_CONFIG, value = "1")
    )
    static class GroupListener {
        final List<String> received = new CopyOnWriteArrayList<>()

        @Topic('pause-unsupported-group')
        void receive(String message) {
            received << message
            if (message == 'ERROR') {
                throw new RuntimeException('Still failing')
            }
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaPauseUnsupportedSpec')
    @KafkaListener(clientId = 'pause-unsupported-registry', offsetReset = EARLIEST, uniqueGroupId = true)
    static class RegistryListener {
        final List<String> received = new CopyOnWriteArrayList<>()

        @Topic('pause-unsupported-registry')
        void receive(String message) {
            received << message
        }
    }

    /**
     * Creates consumers that cannot pause or wake up, like Oracle's OKafka consumer.
     */
    @Singleton
    @Replaces(DefaultConsumerFactory)
    @Requires(property = 'spec.name', value = 'KafkaPauseUnsupportedSpec')
    static class UnsupportedPauseConsumerFactory implements ConsumerFactory {

        final Map<String, AtomicInteger> polls = new java.util.concurrent.ConcurrentHashMap<>()

        @Override
        @SuppressWarnings('unchecked')
        <K, V> Consumer<K, V> createConsumer(Properties config, @Nullable Deserializer<K> kd, @Nullable Deserializer<V> vd) {
            KafkaConsumer<K, V> consumer = new KafkaConsumer<>(config, kd, vd)
            AtomicInteger consumerPolls = polls.computeIfAbsent(config.getProperty(ConsumerConfig.CLIENT_ID_CONFIG) ?: '', { new AtomicInteger() })
            return (Consumer<K, V>) Proxy.newProxyInstance(
                KafkaPauseUnsupportedSpec.classLoader,
                [Consumer] as Class<?>[],
                { proxy, method, args ->
                    if (method.name in ['pause', 'resume', 'paused', 'wakeup']) {
                        throw new RuntimeException('This feature is not supported')
                    }
                    if (method.name == 'poll') {
                        consumerPolls.incrementAndGet()
                    }
                    try {
                        return method.invoke(consumer, args)
                    } catch (InvocationTargetException e) {
                        throw e.cause
                    }
                }
            )
        }
    }
}
