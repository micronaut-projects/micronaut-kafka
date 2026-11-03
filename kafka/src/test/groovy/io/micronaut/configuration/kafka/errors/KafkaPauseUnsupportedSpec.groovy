package io.micronaut.configuration.kafka.errors

import io.micronaut.configuration.kafka.AbstractKafkaContainerSpec
import io.micronaut.configuration.kafka.ConsumerFactory
import io.micronaut.configuration.kafka.DefaultConsumerFactory
import io.micronaut.configuration.kafka.annotation.ErrorStrategy
import io.micronaut.configuration.kafka.annotation.KafkaClient
import io.micronaut.configuration.kafka.annotation.KafkaListener
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

import static io.micronaut.configuration.kafka.annotation.ErrorStrategyValue.RETRY_ON_ERROR
import static io.micronaut.configuration.kafka.annotation.OffsetReset.EARLIEST
import static io.micronaut.configuration.kafka.annotation.OffsetStrategy.SYNC

class KafkaPauseUnsupportedSpec extends AbstractKafkaContainerSpec {

    void "a #mode consumer that cannot pause stops when retries are exhausted instead of retrying again"() {
        given:
        UnsupportedPauseClient client = context.getBean(UnsupportedPauseClient)
        def listener = context.getBean(listenerType)

        when:
        client.send(topic, 'ERROR')

        then: "the record is tried once and retried twice"
        conditions.eventually {
            listener.received == ['ERROR', 'ERROR', 'ERROR']
        }

        when: "more time passes than many retry delays"
        sleep(2000)

        then: "the consumer stopped instead of retrying the record again"
        listener.received == ['ERROR', 'ERROR', 'ERROR']

        where:
        mode     | topic                             | listenerType
        'single' | 'pause-unsupported-stop-single'   | StopOnExhaustedSingleListener
        'batch'  | 'pause-unsupported-stop-batch'    | StopOnExhaustedBatchListener
    }

    @Requires(property = 'spec.name', value = 'KafkaPauseUnsupportedSpec')
    @KafkaClient
    static interface UnsupportedPauseClient {
        void send(@Topic String topic, String message)
    }

    @Requires(property = 'spec.name', value = 'KafkaPauseUnsupportedSpec')
    @KafkaListener(
        offsetReset = EARLIEST,
        offsetStrategy = SYNC,
        uniqueGroupId = true,
        errorStrategy = @ErrorStrategy(value = RETRY_ON_ERROR, retryCount = 2, retryDelay = "50ms", stopOnExhaustedRetry = true),
        properties = @Property(name = ConsumerConfig.MAX_POLL_RECORDS_CONFIG, value = "1")
    )
    static class StopOnExhaustedSingleListener {
        final List<String> received = new CopyOnWriteArrayList<>()

        @Topic('pause-unsupported-stop-single')
        void receive(String message) {
            received << message
            throw new RuntimeException('Still failing')
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaPauseUnsupportedSpec')
    @KafkaListener(
        batch = true,
        offsetReset = EARLIEST,
        offsetStrategy = SYNC,
        uniqueGroupId = true,
        errorStrategy = @ErrorStrategy(value = RETRY_ON_ERROR, retryCount = 2, retryDelay = "50ms", stopOnExhaustedRetry = true)
    )
    static class StopOnExhaustedBatchListener {
        final List<String> received = new CopyOnWriteArrayList<>()

        @Topic('pause-unsupported-stop-batch')
        void receive(List<String> messages) {
            received.addAll(messages)
            throw new RuntimeException('Still failing')
        }
    }

    /**
     * Creates consumers that cannot pause or wake up, like Oracle's OKafka consumer.
     */
    @Singleton
    @Replaces(DefaultConsumerFactory)
    @Requires(property = 'spec.name', value = 'KafkaPauseUnsupportedSpec')
    static class UnsupportedPauseConsumerFactory implements ConsumerFactory {

        @Override
        @SuppressWarnings('unchecked')
        <K, V> Consumer<K, V> createConsumer(Properties config, @Nullable Deserializer<K> kd, @Nullable Deserializer<V> vd) {
            KafkaConsumer<K, V> consumer = new KafkaConsumer<>(config, kd, vd)
            return (Consumer<K, V>) Proxy.newProxyInstance(
                KafkaPauseUnsupportedSpec.classLoader,
                [Consumer] as Class<?>[],
                { proxy, method, args ->
                    if (method.name in ['pause', 'resume', 'paused', 'wakeup']) {
                        throw new RuntimeException('This feature is not supported')
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
