package io.micronaut.configuration.kafka.annotation

import groovy.util.logging.Slf4j
import io.micronaut.configuration.kafka.KafkaConsumerFactory
import io.micronaut.configuration.kafka.KafkaProducerFactory
import io.micronaut.configuration.kafka.annotation.KafkaClient
import io.micronaut.configuration.kafka.annotation.KafkaKey
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.OffsetReset
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.configuration.kafka.config.AbstractKafkaConfiguration
import io.micronaut.configuration.kafka.config.KafkaConsumerConfiguration
import io.micronaut.configuration.kafka.config.KafkaProducerConfiguration
import io.micronaut.context.ApplicationContext
import io.micronaut.core.util.CollectionUtils
import io.micronaut.messaging.annotation.SendTo
import io.opentracing.mock.MockTracer
import org.apache.kafka.common.serialization.BytesDeserializer
import org.apache.kafka.common.serialization.BytesSerializer
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification
import spock.util.concurrent.PollingConditions

import java.util.concurrent.ConcurrentLinkedDeque

class KafkaProducerSpec extends Specification {

    public static final String TOPIC_BLOCKING = "ProducerSpec-users-blocking"
    public static final String TOPIC_QUANTITY = "ProducerSpec-users-quantity"

    @Shared
    MockTracer mockTracer = new MockTracer()

    @Shared
    @AutoCleanup
    ApplicationContext context = ApplicationContext.build(
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
                    "kafka.bootstrap.servers", 'localhost:${random.port}',
                    AbstractKafkaConfiguration.EMBEDDED, true,
                    AbstractKafkaConfiguration.EMBEDDED_TOPICS, [
                    TOPIC_BLOCKING
            ]
            )
    ).singletons(mockTracer).start()


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

        then:
        conditions.eventually {
            userListener.brands['Apple'] == 'iMac'
            userListener.others['Other'] == 'Stuff'
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
}