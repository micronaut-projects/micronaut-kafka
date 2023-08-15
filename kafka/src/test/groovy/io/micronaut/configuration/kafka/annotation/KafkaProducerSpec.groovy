package io.micronaut.configuration.kafka.annotation

import groovy.util.logging.Slf4j
import io.micronaut.configuration.kafka.AbstractKafkaContainerSpec
import io.micronaut.context.annotation.Requires
import io.micronaut.context.event.BeanCreatedEvent
import io.micronaut.context.event.BeanCreatedEventListener
import io.micronaut.messaging.MessageHeaders
import io.micronaut.messaging.annotation.MessageHeader
import io.micronaut.messaging.annotation.SendTo
import jakarta.inject.Singleton
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
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
    public static final String TOPIC_RECORDS = "KafkaProducerSpec-records"
    public static final String TOPIC_RECORDS_BATCH = "KafkaProducerSpec-records-batch"

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
         (EMBEDDED_TOPICS)                           : [TOPIC_BLOCKING, TOPIC_RECORDS, TOPIC_RECORDS_BATCH]]
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

    void "test send producer record"() {
        given:
        ProducerRecordClient client = context.getBean(ProducerRecordClient)
        ConsumerRecordListener listener = context.getBean(ConsumerRecordListener)

        when:
        client.sendRecord("c", "DEFAULT KEY", new ProducerRecord<>("", "one", "ONE"))
        client.sendRecord("c", "DEFAULT KEY", new ProducerRecord<>("", "TWO"))
        client.sendRecord("c", "DEFAULT KEY", new ProducerRecord<>("", null, "three", "THREE", [
                new RecordHeader("A", "a2".bytes),
                new RecordHeader("B", "b2".bytes),
                new RecordHeader("C", "c2".bytes),
                new RecordHeader("D", "d2".bytes)]))

        then:
        conditions.eventually {
            listener.records.size() == 3
            listener.records[0].key() == "one"
            listener.records[0].value() == "ONE"
            listener.records[0].topic() == TOPIC_RECORDS
            listener.records[0].partition() == 0
            listener.records[0].timestamp() > 0
            listener.records[0].headers().headers("A").any { it.value() == "a".bytes }
            listener.records[0].headers().headers("B").any { it.value() == "b".bytes }
            listener.records[0].headers().headers("C").any { it.value() == "c".bytes }
            listener.records[1].key() == "DEFAULT KEY"
            listener.records[1].value() == "TWO"
            listener.records[1].topic() == TOPIC_RECORDS
            listener.records[1].partition() == 0
            listener.records[1].timestamp() > 0
            listener.records[1].headers().headers("A").any { it.value() == "a".bytes }
            listener.records[1].headers().headers("B").any { it.value() == "b".bytes }
            listener.records[1].headers().headers("C").any { it.value() == "c".bytes }
            listener.records[2].key() == "three"
            listener.records[2].value() == "THREE"
            listener.records[2].topic() == TOPIC_RECORDS
            listener.records[2].partition() == 0
            listener.records[2].timestamp() > 0
            listener.records[2].headers().headers("A").any { it.value() == "a".bytes }
            listener.records[2].headers().headers("B").any { it.value() == "b".bytes }
            listener.records[2].headers().headers("C").any { it.value() == "c".bytes }
            listener.records[2].headers().headers("A").any { it.value() == "a2".bytes }
            listener.records[2].headers().headers("B").any { it.value() == "b2".bytes }
            listener.records[2].headers().headers("C").any { it.value() == "c2".bytes }
            listener.records[2].headers().headers("D").any { it.value() == "d2".bytes }
        }
    }

    void "test batch send producer record"() {
        given:
        ProducerRecordBatchClient client = context.getBean(ProducerRecordBatchClient)
        ConsumerRecordListener listener = context.getBean(ConsumerRecordListener)

        when:
        client.sendRecords("c", "DEFAULT KEY", List.of(
                new ProducerRecord<>("", "one", "ONE"),
                new ProducerRecord<>("", "TWO"),
                new ProducerRecord<>("", null, "three", "THREE", [
                        new RecordHeader("A", "a2".bytes),
                        new RecordHeader("B", "b2".bytes),
                        new RecordHeader("C", "c2".bytes),
                        new RecordHeader("D", "d2".bytes)])))

        then:
        conditions.eventually {
            listener.batch.size() == 3
            listener.batch[0].key() == "one"
            listener.batch[0].value() == "ONE"
            listener.batch[0].topic() == TOPIC_RECORDS_BATCH
            listener.batch[0].partition() == 0
            listener.batch[0].timestamp() > 0
            listener.batch[0].headers().headers("A").any { it.value() == "a".bytes }
            listener.batch[0].headers().headers("B").any { it.value() == "b".bytes }
            listener.batch[0].headers().headers("C").any { it.value() == "c".bytes }
            listener.batch[1].key() == "DEFAULT KEY"
            listener.batch[1].value() == "TWO"
            listener.batch[1].topic() == TOPIC_RECORDS_BATCH
            listener.batch[1].partition() == 0
            listener.batch[1].timestamp() > 0
            listener.batch[1].headers().headers("A").any { it.value() == "a".bytes }
            listener.batch[1].headers().headers("B").any { it.value() == "b".bytes }
            listener.batch[1].headers().headers("C").any { it.value() == "c".bytes }
            listener.batch[2].key() == "three"
            listener.batch[2].value() == "THREE"
            listener.batch[2].topic() == TOPIC_RECORDS_BATCH
            listener.batch[2].partition() == 0
            listener.batch[2].timestamp() > 0
            listener.batch[2].headers().headers("A").any { it.value() == "a".bytes }
            listener.batch[2].headers().headers("B").any { it.value() == "b".bytes }
            listener.batch[2].headers().headers("C").any { it.value() == "c".bytes }
            listener.batch[2].headers().headers("A").any { it.value() == "a2".bytes }
            listener.batch[2].headers().headers("B").any { it.value() == "b2".bytes }
            listener.batch[2].headers().headers("C").any { it.value() == "c2".bytes }
            listener.batch[2].headers().headers("D").any { it.value() == "d2".bytes }
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

    @Requires(property = 'spec.name', value = 'KafkaProducerSpec')
    @KafkaClient
    @MessageHeader(name = "A", value = "a")
    static interface ProducerRecordClient {
        @Topic(TOPIC_RECORDS)
        @MessageHeader(name = "B", value = "b")
        String sendRecord(@MessageHeader(name = "C") String header, @KafkaKey String key, ProducerRecord<String, String> record)
    }

    @Requires(property = 'spec.name', value = 'KafkaProducerSpec')
    @KafkaClient(batch = true)
    @MessageHeader(name = "A", value = "a")
    static interface ProducerRecordBatchClient {
        @Topic(TOPIC_RECORDS_BATCH)
        @MessageHeader(name = "B", value = "b")
        String sendRecords(@MessageHeader(name = "C") String header, @KafkaKey String key, List<ProducerRecord<String, String>> records)
    }

    @Requires(property = 'spec.name', value = 'KafkaProducerSpec')
    @KafkaListener(offsetReset = EARLIEST)
    static class ConsumerRecordListener {
        List<ConsumerRecord<String, String>> records = []
        List<ConsumerRecord<String, String>> batch = []
        @Topic(TOPIC_RECORDS)
        void receive(ConsumerRecord<String, String> record) { records << record }
        @Topic(TOPIC_RECORDS_BATCH)
        void receiveBatch(ConsumerRecord<String, String> record) { batch << record }
    }
}
