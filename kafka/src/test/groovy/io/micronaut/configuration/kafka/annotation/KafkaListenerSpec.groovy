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

import groovy.transform.EqualsAndHashCode
import io.micrometer.core.instrument.MeterRegistry
import io.micronaut.configuration.kafka.config.AbstractKafkaConfiguration
import io.micronaut.configuration.kafka.config.AbstractKafkaProducerConfiguration
import io.micronaut.configuration.kafka.health.KafkaHealthIndicator
import io.micronaut.configuration.kafka.metrics.KafkaConsumerMetrics
import io.micronaut.configuration.kafka.metrics.KafkaProducerMetrics
import io.micronaut.configuration.kafka.serde.JsonSerde
import io.micronaut.configuration.metrics.management.endpoint.MetricsEndpoint
import io.micronaut.context.ApplicationContext
import io.micronaut.core.util.CollectionUtils
import io.micronaut.http.client.DefaultHttpClientConfiguration
import io.micronaut.http.client.RxHttpClient
import io.micronaut.messaging.MessageHeaders
import io.micronaut.messaging.annotation.Body
import io.micronaut.messaging.annotation.Header
import io.micronaut.runtime.server.EmbeddedServer
import io.reactivex.Single
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.StringSerializer
import org.testcontainers.containers.KafkaContainer
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Stepwise
import spock.util.concurrent.PollingConditions

@Stepwise
class KafkaListenerSpec extends Specification {

    @Shared @AutoCleanup KafkaContainer kafkaContainer = new KafkaContainer()
    @Shared @AutoCleanup ApplicationContext context
    @Shared
    @AutoCleanup
    EmbeddedServer embeddedServer
    @Shared
    @AutoCleanup
    RxHttpClient httpClient

    def setupSpec() {
        kafkaContainer.start()
        embeddedServer = ApplicationContext.run(EmbeddedServer,
                CollectionUtils.mapOf(
                        "kafka.bootstrap.servers", kafkaContainer.getBootstrapServers(),
                        "micrometer.metrics.enabled", true,
                        'endpoints.metrics.sensitive', false,
                        AbstractKafkaConfiguration.EMBEDDED_TOPICS, ["words", "books", "words-records", "books-records"]
                )
        )
        context = embeddedServer.applicationContext
        httpClient = embeddedServer.applicationContext.createBean(RxHttpClient, embeddedServer.getURL(), new DefaultHttpClientConfiguration(followRedirects: false))
    }



    void "test simple consumer"() {
        given:
        PollingConditions conditions = new PollingConditions(timeout: 30, delay: 1)
        MyClient myClient = context.getBean(MyClient)
        MyConsumer myConsumer = context.getBean(MyConsumer)
        context.containsBean(KafkaHealthIndicator)

        expect:
        context.containsBean(KafkaConsumerMetrics)
        context.containsBean(KafkaProducerMetrics)
        context.containsBean(MeterRegistry)
        context.containsBean(MetricsEndpoint)

        when:
        myClient.sendSentence("key", "hello world", "words")

        then:
        conditions.eventually {
            myConsumer.wordCount == 2
            myConsumer.lastTopic == 'words'
        }

        and:
        conditions.eventually {
            def response = httpClient.exchange("/metrics", Map).blockingFirst()
            Map result = response.body()

            result.names.contains("kafka.producer.count")
            !result.names.contains("kafka.count")
        }
    }

    void "test POJO consumer"() {
        when:
        MyClient myClient = context.getBean(MyClient)
        Book book = myClient.sendReactive("Stephen King", new Book(title: "The Stand")).blockingGet()

        PollingConditions conditions = new PollingConditions(timeout: 30, delay: 1)

        PojoConsumer myConsumer = context.getBean(PojoConsumer)
        then:
        conditions.eventually {
            myConsumer.lastBook == book
            myConsumer.messageHeaders != null
        }
    }


    void "test @KafkaKey annotation"() {
        when:
        MyClient myClient = context.getBean(MyClient)
        RecordMetadata metadata = myClient.sendGetRecordMetadata("key", "hello world")

        PollingConditions conditions = new PollingConditions(timeout: 30, delay: 1)

        MyConsumer2 myConsumer = context.getBean(MyConsumer2)
        then:
        metadata != null
        metadata.topic() == "words"
        conditions.eventually {
            myConsumer.wordCount == 4
            myConsumer.key == "key"
        }
    }

    void "test @Body annotation"() {
        when:
        MyClient myClient = context.getBean(MyClient)
        RecordMetadata metadata = myClient.sendGetRecordMetadata("key", "hello world")

        PollingConditions conditions = new PollingConditions(timeout: 30, delay: 1)

        MyConsumer4 myConsumer = context.getBean(MyConsumer4)
        then:
        metadata != null
        metadata.topic() == "words"
        conditions.eventually {
            myConsumer.body == "hello world"
        }
    }

    void "test receive ConsumerRecord"() {
        when:
        def config = context.getBean(AbstractKafkaProducerConfiguration)
        config.setKeySerializer(new StringSerializer())
        config.setValueSerializer(new StringSerializer())
        Producer producer = context.createBean(Producer, config)
        producer.send(new ProducerRecord("words-records", "key", "hello world")).get()

        PollingConditions conditions = new PollingConditions(timeout: 30, delay: 1)

        MyConsumer3 myConsumer = context.getBean(MyConsumer3)
        then:
        conditions.eventually {
            myConsumer.wordCount == 2
            myConsumer.key == "key"
        }

        cleanup:
        producer?.close()

    }


    void "test POJO consumer record"() {

        when:
        def config = context.getBean(AbstractKafkaProducerConfiguration)
        config.setKeySerializer(new StringSerializer())
        config.setValueSerializer(new JsonSerde(Book).serializer())
        Producer producer = context.createBean(Producer, config)
        producer.send(new ProducerRecord("books-records", "Stephen King", new Book(title: "The Stand"))).get()

        PollingConditions conditions = new PollingConditions(timeout: 30, delay: 1)

        PojoConsumer2 myConsumer = context.getBean(PojoConsumer2)
        then:
        conditions.eventually {
            myConsumer.lastBook == new Book(title: "The Stand")
            myConsumer.topic == "books-records"
            myConsumer.offset != null
        }

        cleanup:
        producer?.close()

    }

    void "test @Header annotation with optional"() {
        when:
        MyClient myClient = context.getBean(MyClient)
        myClient.sendSentence("key", "Hello, world!", "words")

        PollingConditions conditions = new PollingConditions(timeout: 30, delay: 1)

        MyConsumer5 myConsumer = context.getBean(MyConsumer5)
        then:
        conditions.eventually {
            myConsumer.sentence == "Hello, world!"
            myConsumer.missingHeader
            myConsumer.topic == "words"
        }
    }

    @KafkaClient
    static interface MyClient {
        @Topic("words")
        void sendSentence(@KafkaKey String key, String sentence, @Header String topic)

        @Topic("words")
        RecordMetadata sendGetRecordMetadata(@KafkaKey String key, String sentence)

        @Topic("books")
        Single<Book> sendReactive(@KafkaKey String key, Book book)
    }

    @KafkaListener(offsetReset = OffsetReset.EARLIEST)
    static class MyConsumer {
        int wordCount
        String lastTopic

        @Topic("words")
        void countWord(String sentence, @Header String topic) {
            wordCount += sentence.split(/\s/).size()
            lastTopic = topic
        }
    }

    @KafkaListener(offsetReset = OffsetReset.EARLIEST)
    static class MyConsumer2 {
        int wordCount
        String key

        @Topic("words")
        void countWord(@KafkaKey String key, String sentence) {
            wordCount += sentence.split(/\s/).size()
            this.key = key
        }
    }

    @KafkaListener(offsetReset = OffsetReset.EARLIEST)
    static class MyConsumer3 {
        int wordCount
        String key

        @Topic("words-records")
        void countWord(@KafkaKey String key, ConsumerRecord<String, String> record) {
            wordCount += record.value().split(/\s/).size()
            this.key = key
        }
    }

    @KafkaListener(offsetReset = OffsetReset.EARLIEST)
    static class MyConsumer4 {
        String body

        @Topic("words")
        void countWord(@Body String body) {
            this.body = body
        }
    }

    @KafkaListener(offsetReset = OffsetReset.EARLIEST)
    static class MyConsumer5 {
        boolean missingHeader
        String sentence
        String topic

        @Topic("words")
        void countWord(@Body String sentence, @Header Optional<String> topic, @Header Optional<String> missing) {
            missingHeader = !missing.isPresent()
            this.sentence = sentence
            this.topic = topic.get()
        }
    }

    @KafkaListener(offsetReset = OffsetReset.EARLIEST)
    static class PojoConsumer2 {
        Book lastBook
        String topic
        Long offset

        @Topic("books-records")
        void receiveBook(String topic, long offset, ConsumerRecord<String, Book> record) {
            lastBook = record.value()
            this.topic = topic
            this.offset = offset
        }
    }

    @KafkaListener(offsetReset = OffsetReset.EARLIEST)
    static class PojoConsumer {
        Book lastBook
        MessageHeaders messageHeaders

        @Topic("books")
        void receiveBook(Book book, MessageHeaders messageHeaders) {
            lastBook = book
            this.messageHeaders = messageHeaders

        }
    }

    @EqualsAndHashCode
    static class Book {
        String title
    }
}
