
package io.micronaut.configuration.kafka.annotation

import groovy.util.logging.Slf4j
import io.micronaut.configuration.kafka.config.AbstractKafkaConfiguration
import io.micronaut.context.ApplicationContext
import io.micronaut.context.event.BeanCreatedEvent
import io.micronaut.context.event.BeanCreatedEventListener
import io.micronaut.core.util.CollectionUtils
import io.micronaut.messaging.MessageHeaders
import io.micronaut.messaging.annotation.SendTo
import io.opentracing.mock.MockTracer
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.header.internals.RecordHeaders
import org.testcontainers.containers.KafkaContainer
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.producer.Producer
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification
import spock.util.concurrent.PollingConditions

import javax.inject.Singleton
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.atomic.AtomicInteger

class KafkaProducerSpec extends Specification {

    public static final String TOPIC_BLOCKING = "ProducerSpec-users-blocking"
    public static final String TOPIC_QUANTITY = "ProducerSpec-users-quantity"

    @Shared
    MockTracer mockTracer = new MockTracer()

    @Shared @AutoCleanup KafkaContainer kafkaContainer = new KafkaContainer()

    @Shared
    @AutoCleanup
    ApplicationContext context

    def setupSpec() {
        kafkaContainer.start()
        context =  ApplicationContext.build(
                CollectionUtils.mapOf(
                        'micronaut.application.name', 'test-app',
                        "kafka.schema.registry.url", "http://localhot:8081",
                        "kafka.producers.named.key.serializer", "org.apache.kafka.common.serialization.StringSerializer",
                        "kafka.producers.named.value.serializer", "org.apache.kafka.common.serialization.StringSerializer",
                        "kafka.producers.default.key.serializer", "org.apache.kafka.common.serialization.StringSerializer",
                        "kafka.producers.default.key-serializer", "org.apache.kafka.common.serialization.StringSerializer",
                        "kafka.producers.default.keySerializer", "org.apache.kafka.common.serialization.StringSerializer",
                        "kafka.producers.default.value.serializer", "org.apache.kafka.common.serialization.StringSerializer",
                        "kafka.producers.default.value-serializer", "org.apache.kafka.common.serialization.StringSerializer",
                        "kafka.producers.default.valueSerializer", "org.apache.kafka.common.serialization.StringSerializer",
                        "kafka.consumers.default.key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer",
                        "kafka.consumers.default.key-deserializer", "org.apache.kafka.common.serialization.StringDeserializer",
                        "kafka.consumers.default.keyDeserializer", "org.apache.kafka.common.serialization.StringDeserializer",
                        "kafka.consumers.default.value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer",
                        "kafka.consumers.default.value-deserializer", "org.apache.kafka.common.serialization.StringDeserializer",
                        "kafka.consumers.default.valueDeserializer", "org.apache.kafka.common.serialization.StringDeserializer",
                        "kafka.bootstrap.servers", kafkaContainer.getBootstrapServers(),
                        AbstractKafkaConfiguration.EMBEDDED_TOPICS, [
                        TOPIC_BLOCKING
                ]
                )
        ).singletons(mockTracer).start()
    }


    def "test customize defaults"() {
        given:
        UserClient client = context.getBean(UserClient)
        UserListener userListener = context.getBean(UserListener)
        userListener.users.clear()
        userListener.keys.clear()
        PollingConditions conditions = new PollingConditions(timeout: 30, delay: 1)

        when:
        client.sendUser("Bob", "Robert")

        then:
        conditions.eventually {
            mockTracer.finishedSpans().size() > 0
            userListener.keys.size() == 1
            userListener.keys.iterator().next() == "Bob"
            userListener.users.size() == 1
            userListener.users.iterator().next() == "Robert"
        }
    }

    def "test multiple consumer methods"() {
        given:
        ProductClient client = context.getBean(ProductClient)
        ProductListener userListener = context.getBean(ProductListener)
        userListener.brands.clear()
        userListener.others.clear()
        PollingConditions conditions = new PollingConditions(timeout: 30, delay: 1)

        when:
        client.send("Apple", "iMac")
        client.send2("Other", "Stuff")
        client.send3("ProducerSpec-my-products-2", "Dell", "PC")

        then:
        conditions.eventually {
            userListener.brands['Apple'] == 'iMac'
            userListener.others['Other'] == 'Stuff'
            userListener.others['Dell'] == 'PC'
        }
        and:
            context.getBean(KafkaConsumerInstrumentation).counter.get() > 0
            context.getBean(KafkaProducerInstrumentation).counter.get() > 0
    }

    def "test collection of headers"() {
        given:
        BicycleClient client = context.getBean(BicycleClient)
        BicycleListener listener = context.getBean(BicycleListener)
        listener.brands.clear()
        listener.others.clear()
        listener.additionalInfo.clear()
        PollingConditions conditions = new PollingConditions(timeout: 30, delay: 1)

        when:
        client.send("Raleigh", "Professional", [new RecordHeader("size", "60cm".bytes)])

        then:
        conditions.eventually {
            mockTracer.finishedSpans().size() > 0
            listener.brands.size() == 1
            listener.brands["Raleigh"] == "Professional"
            listener.others.isEmpty()
            !listener.additionalInfo.isEmpty()
            listener.additionalInfo["size"] == "60cm"
        }
    }

    def "test kafka record headers"() {
        given:
        BicycleClient client = context.getBean(BicycleClient)
        BicycleListener listener = context.getBean(BicycleListener)
        listener.brands.clear()
        listener.others.clear()
        listener.additionalInfo.clear()
        PollingConditions conditions = new PollingConditions(timeout: 30, delay: 1)

        when:
        client.send2("Raleigh", "International", new RecordHeaders([new RecordHeader("year", "1971".bytes)]))

        then:
        conditions.eventually {
            mockTracer.finishedSpans().size() > 0
            listener.brands.isEmpty()
            listener.others.size() == 1
            listener.others["Raleigh"] == "International"
            !listener.additionalInfo.isEmpty()
            listener.additionalInfo["year"] == "1971"
        }
    }

    @KafkaClient(acks = KafkaClient.Acknowledge.ALL, id = "named")
    static interface NamedClient {
        @Topic(KafkaProducerSpec.TOPIC_BLOCKING)
        String sendUser(@KafkaKey String name, String user)
    }

    @KafkaClient(acks = KafkaClient.Acknowledge.ALL)
    static interface UserClient {
        @Topic(KafkaProducerSpec.TOPIC_BLOCKING)
        String sendUser(@KafkaKey String name, String user)
    }

    @KafkaClient(acks = KafkaClient.Acknowledge.ALL)
    static interface ProductClient {
        @Topic("ProducerSpec-my-products")
        void send(@KafkaKey String brand, String name)

        @Topic("ProducerSpec-my-products-2")
        void send2(@KafkaKey String key, String name)

        void send3(@Topic String topic, @KafkaKey String key, String name)
    }

    @KafkaClient(acks = KafkaClient.Acknowledge.ALL)
    static interface BicycleClient {
        @Topic("ProducerSpec-my-bicycles")
        void send(@KafkaKey String brand, String name, Collection<Header> headers)

        @Topic("ProducerSpec-my-bicycles-2")
        void send2(@KafkaKey String key, String name, Headers headers)
    }

    @KafkaListener(offsetReset = OffsetReset.EARLIEST)
    static class UserListener {
        Queue<String> users = new ConcurrentLinkedDeque<>()
        Queue<String> keys = new ConcurrentLinkedDeque<>()

        @Topic(KafkaProducerSpec.TOPIC_BLOCKING)
        @SendTo(KafkaProducerSpec.TOPIC_QUANTITY)
        String receive(@KafkaKey String key, String user) {
            users << user
            keys << key
            return user
        }
    }

    @KafkaListener(offsetReset = OffsetReset.EARLIEST)
    @Slf4j
    static class ProductListener {

        Map<String, String> brands = [:]
        Map<String, String> others = [:]
        @Topic("ProducerSpec-my-products")
        void receive(@KafkaKey String brand, String name) {
            log.info("Got Product - {} by {}", brand, name)
            brands[brand] = name
        }

        @Topic("ProducerSpec-my-products-2")
        void receive2(@KafkaKey String key, String name) {
            log.info("Got Lineup info - {} by {}", key, name)
            others[key] = name
        }
    }

    @KafkaListener(offsetReset = OffsetReset.EARLIEST)
    @Slf4j
    static class BicycleListener {

        Map<String, String> brands = [:]
        Map<String, String> others = [:]
        Map<String, String> additionalInfo = [:]

        @Topic("ProducerSpec-my-bicycles")
        void receive(@KafkaKey String brand, String name, MessageHeaders messageHeaders) {
            log.info("Got Bicycle - {} by {}", brand, name)
            brands[brand] = name
            messageHeaders.each { header -> additionalInfo.put(header.key, new String(header.value[0])) }
        }

        @Topic("ProducerSpec-my-bicycles-2")
        void receive2(@KafkaKey String key, String name, MessageHeaders messageHeaders) {
            log.info("Got Bicycle info - {} by {}", key, name)
            others[key] = name
            messageHeaders.each { header -> additionalInfo.put(header.key, new String(header.value[0])) }
        }
    }

    @Singleton
    static class KafkaConsumerInstrumentation implements BeanCreatedEventListener<Consumer<?, ?>> {

        final AtomicInteger counter = new AtomicInteger(0);

        @Override
        Consumer<?, ?> onCreated(BeanCreatedEvent<Consumer<?, ?>> event) {
            counter.incrementAndGet()
            return event.getBean()
        }
    }

    @Singleton
    static class KafkaProducerInstrumentation implements BeanCreatedEventListener<Producer<?, ?>> {

        final AtomicInteger counter = new AtomicInteger(0);

        @Override
        Producer<?, ?> onCreated(BeanCreatedEvent<Producer<?, ?>> event) {
            counter.incrementAndGet()
            return event.getBean()
        }

    }
}