package io.micronaut.configuration.kafka.annotation

import groovy.util.logging.Slf4j
import io.micronaut.configuration.kafka.AbstractKafkaContainerSpec
import io.micronaut.context.annotation.Requires
import io.micronaut.context.event.BeanCreatedEvent
import io.micronaut.context.event.BeanCreatedEventListener
import io.micronaut.messaging.MessageHeaders
import io.micronaut.messaging.annotation.SendTo
import jakarta.inject.Singleton
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer

import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.atomic.AtomicInteger

import static io.micronaut.configuration.kafka.annotation.KafkaClient.Acknowledge.ALL
import static io.micronaut.configuration.kafka.annotation.OffsetReset.EARLIEST
import static io.micronaut.configuration.kafka.config.AbstractKafkaConfiguration.EMBEDDED_TOPICS

class KafkaProducerSpec extends AbstractKafkaContainerSpec {

    public static final String TOPIC_BLOCKING = "KafkaProducerSpec-users-blocking"
    public static final String TOPIC_QUANTITY = "KafkaProducerSpec-users-quantity"

    Map<String, Object> getConfiguration() {
        super.configuration +
        ['micronaut.application.name'                : 'test-app',
         "kafka.schema.registry.url"                 : "http://localhot:8081",
         "kafka.producers.named.key.serializer"      : StringSerializer.name,
         "kafka.producers.named.value.serializer"    : StringSerializer.name,
         "kafka.producers.default.key.serializer"    : StringSerializer.name,
         "kafka.producers.default.key-serializer"    : StringSerializer.name,
         "kafka.producers.default.keySerializer"     : StringSerializer.name,
         "kafka.producers.default.value.serializer"  : StringSerializer.name,
         "kafka.producers.default.value-serializer"  : StringSerializer.name,
         "kafka.producers.default.valueSerializer"   : StringSerializer.name,
         "kafka.consumers.default.key.deserializer"  : StringDeserializer.name,
         "kafka.consumers.default.key-deserializer"  : StringDeserializer.name,
         "kafka.consumers.default.keyDeserializer"   : StringDeserializer.name,
         "kafka.consumers.default.value.deserializer": StringDeserializer.name,
         "kafka.consumers.default.value-deserializer": StringDeserializer.name,
         "kafka.consumers.default.valueDeserializer" : StringDeserializer.name,
         (EMBEDDED_TOPICS)                           : [TOPIC_BLOCKING]]
    }

    void "test customize defaults"() {
        given:
        UserClient client = context.getBean(UserClient)
        UserListener userListener = context.getBean(UserListener)
        userListener.users.clear()
        userListener.keys.clear()

        when:
        client.sendUser("Bob", "Robert")

        then:
        conditions.eventually {
            userListener.keys.size() == 1
            userListener.keys.iterator().next() == "Bob"
            userListener.users.size() == 1
            userListener.users.iterator().next() == "Robert"
        }
    }

    void "test multiple consumer methods"() {
        given:
        ProductClient client = context.getBean(ProductClient)
        ProductListener userListener = context.getBean(ProductListener)
        userListener.brands.clear()
        userListener.others.clear()

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

    void "test collection of headers"() {
        given:
        BicycleClient client = context.getBean(BicycleClient)
        BicycleListener listener = context.getBean(BicycleListener)
        listener.brands.clear()
        listener.others.clear()
        listener.additionalInfo.clear()

        when:
        client.send("Raleigh", "Professional", [new RecordHeader("size", "60cm".bytes)])

        then:
        conditions.eventually {
            listener.brands.size() == 1
            listener.brands["Raleigh"] == "Professional"
            listener.others.isEmpty()
            !listener.additionalInfo.isEmpty()
            listener.additionalInfo["size"] == "60cm"
        }
    }

    void "test kafka record headers"() {
        given:
        BicycleClient client = context.getBean(BicycleClient)
        BicycleListener listener = context.getBean(BicycleListener)
        listener.brands.clear()
        listener.others.clear()
        listener.additionalInfo.clear()

        when:
        client.send2("Raleigh", "International", new RecordHeaders([new RecordHeader("year", "1971".bytes)]))

        then:
        conditions.eventually {
            listener.brands.isEmpty()
            listener.others.size() == 1
            listener.others["Raleigh"] == "International"
            !listener.additionalInfo.isEmpty()
            listener.additionalInfo["year"] == "1971"
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaProducerSpec')
    @KafkaClient(acks = ALL, id = "named")
    static interface NamedClient {
        @Topic(KafkaProducerSpec.TOPIC_BLOCKING)
        String sendUser(@KafkaKey String name, String user)
    }

    @Requires(property = 'spec.name', value = 'KafkaProducerSpec')
    @KafkaClient(acks = ALL)
    static interface UserClient {
        @Topic(KafkaProducerSpec.TOPIC_BLOCKING)
        String sendUser(@KafkaKey String name, String user)
    }

    @Requires(property = 'spec.name', value = 'KafkaProducerSpec')
    @KafkaClient(acks = ALL)
    static interface ProductClient {
        @Topic("ProducerSpec-my-products")
        void send(@KafkaKey String brand, String name)

        @Topic("ProducerSpec-my-products-2")
        void send2(@KafkaKey String key, String name)

        void send3(@Topic String topic, @KafkaKey String key, String name)
    }

    @Requires(property = 'spec.name', value = 'KafkaProducerSpec')
    @KafkaClient(acks = ALL)
    static interface BicycleClient {
        @Topic("ProducerSpec-my-bicycles")
        void send(@KafkaKey String brand, String name, Collection<Header> headers)

        @Topic("ProducerSpec-my-bicycles-2")
        void send2(@KafkaKey String key, String name, Headers headers)
    }

    @Requires(property = 'spec.name', value = 'KafkaProducerSpec')
    @KafkaListener(offsetReset = EARLIEST)
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

    @Requires(property = 'spec.name', value = 'KafkaProducerSpec')
    @KafkaListener(offsetReset = EARLIEST)
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

    @Requires(property = 'spec.name', value = 'KafkaProducerSpec')
    @KafkaListener(offsetReset = EARLIEST)
    @Slf4j
    static class BicycleListener {

        Map<String, String> brands = [:]
        Map<String, String> others = [:]
        Map<String, String> additionalInfo = [:]

        @Topic("ProducerSpec-my-bicycles")
        void receive(@KafkaKey String brand, String name, MessageHeaders messageHeaders) {
            log.info("Got Bicycle - {} by {}", brand, name)
            brands[brand] = name
            for (header in messageHeaders) {
                additionalInfo[header.key] = new String(header.value[0])
            }
        }

        @Topic("ProducerSpec-my-bicycles-2")
        void receive2(@KafkaKey String key, String name, MessageHeaders messageHeaders) {
            log.info("Got Bicycle info - {} by {}", key, name)
            others[key] = name
            for (header in messageHeaders) {
                additionalInfo[header.key] = new String(header.value[0])
            }
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaProducerSpec')
    @Singleton
    static class KafkaConsumerInstrumentation implements BeanCreatedEventListener<Consumer<?, ?>> {

        final AtomicInteger counter = new AtomicInteger(0)

        @Override
        Consumer<?, ?> onCreated(BeanCreatedEvent<Consumer<?, ?>> event) {
            counter.incrementAndGet()
            event.bean
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaProducerSpec')
    @Singleton
    static class KafkaProducerInstrumentation implements BeanCreatedEventListener<Producer<?, ?>> {

        final AtomicInteger counter = new AtomicInteger(0)

        @Override
        Producer<?, ?> onCreated(BeanCreatedEvent<Producer<?, ?>> event) {
            counter.incrementAndGet()
            event.bean
        }
    }
}
