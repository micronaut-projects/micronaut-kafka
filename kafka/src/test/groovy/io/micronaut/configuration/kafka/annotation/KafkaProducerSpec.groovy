/*
 * Copyright 2017-2019 original authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.micronaut.configuration.kafka.annotation

import groovy.util.logging.Slf4j
import io.micronaut.configuration.kafka.config.AbstractKafkaConfiguration
import io.micronaut.context.ApplicationContext
import io.micronaut.context.event.BeanCreatedEvent
import io.micronaut.context.event.BeanCreatedEventListener
import io.micronaut.core.util.CollectionUtils
import io.micronaut.messaging.annotation.SendTo
import io.opentracing.mock.MockTracer
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