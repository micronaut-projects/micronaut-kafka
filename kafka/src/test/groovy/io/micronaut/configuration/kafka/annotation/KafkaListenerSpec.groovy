package io.micronaut.configuration.kafka.annotation

import groovy.transform.EqualsAndHashCode
import io.micrometer.core.instrument.MeterRegistry
import io.micronaut.configuration.kafka.AbstractEmbeddedServerSpec
import io.micronaut.configuration.kafka.config.AbstractKafkaProducerConfiguration
import io.micronaut.configuration.kafka.health.KafkaHealthIndicator
import io.micronaut.configuration.kafka.metrics.KafkaConsumerMetrics
import io.micronaut.configuration.kafka.metrics.KafkaProducerMetrics
import io.micronaut.configuration.kafka.serde.JsonSerde
import io.micronaut.configuration.metrics.management.endpoint.MetricsEndpoint
import io.micronaut.context.annotation.Requires
import io.micronaut.http.client.DefaultHttpClientConfiguration
import io.micronaut.http.client.RxHttpClient
import io.micronaut.messaging.MessageHeaders
import io.micronaut.messaging.annotation.MessageBody
import io.micronaut.messaging.annotation.MessageHeader
import io.reactivex.Single
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.StringSerializer
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Stepwise

import io.micronaut.core.annotation.Nullable

import static io.micronaut.configuration.kafka.annotation.OffsetReset.EARLIEST
import static io.micronaut.configuration.kafka.config.AbstractKafkaConfiguration.EMBEDDED_TOPICS

@Stepwise
class KafkaListenerSpec extends AbstractEmbeddedServerSpec {

    @Shared @AutoCleanup RxHttpClient httpClient

    protected Map<String, Object> getConfiguration() {
        super.configuration +
                ['micrometer.metrics.enabled': true,
                 'endpoints.metrics.sensitive': false,
                 (EMBEDDED_TOPICS): ['words', 'books', 'words-records', 'books-records']]
    }

    def setupSpec() {
        httpClient = context.createBean(
                RxHttpClient,
                embeddedServer.getURL(),
                new DefaultHttpClientConfiguration(followRedirects: false)
        )
    }

    void "test simple consumer"() {
        given:
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

        PojoConsumer myConsumer = context.getBean(PojoConsumer)

        then:
        conditions.eventually {
            myConsumer.lastBook == book
            myConsumer.messageHeaders != null
        }
    }

    void "test POJO consumer null with (tombstone) value"() {
        when:
        MyClient myClient = context.getBean(MyClient)
        myClient.sendBook("Stephen King", null)

        PojoConsumer myConsumer = context.getBean(PojoConsumer)

        then:
        conditions.eventually {
            myConsumer.lastBook == null
            myConsumer.messageHeaders != null
        }
    }

    void "test @KafkaKey annotation"() {
        when:
        MyClient myClient = context.getBean(MyClient)
        RecordMetadata metadata = myClient.sendGetRecordMetadata("key", "hello world")

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

        MyConsumer5 myConsumer = context.getBean(MyConsumer5)

        then:
        conditions.eventually {
            myConsumer.sentence == "Hello, world!"
            myConsumer.missingHeader
            myConsumer.topic == "words"
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaListenerSpec')
    @KafkaClient
    static interface MyClient {
        @Topic("words")
        void sendSentence(@KafkaKey String key, String sentence, @MessageHeader String topic)

        @Topic("words")
        RecordMetadata sendGetRecordMetadata(@KafkaKey String key, String sentence)

        @Topic("books")
        Single<Book> sendReactive(@KafkaKey String key, Book book)

        @Topic("books")
        void sendBook(@KafkaKey String key, @Nullable @MessageBody Book book)
    }

    @Requires(property = 'spec.name', value = 'KafkaListenerSpec')
    @KafkaListener(offsetReset = EARLIEST)
    static class MyConsumer {
        int wordCount
        String lastTopic

        @Topic("words")
        void countWord(String sentence, @MessageHeader String topic) {
            wordCount += sentence.split(/\s/).size()
            lastTopic = topic
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaListenerSpec')
    @KafkaListener(offsetReset = EARLIEST)
    static class MyConsumer2 {
        int wordCount
        String key

        @Topic("words")
        void countWord(@KafkaKey String key, String sentence) {
            wordCount += sentence.split(/\s/).size()
            this.key = key
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaListenerSpec')
    @KafkaListener(offsetReset = EARLIEST)
    static class MyConsumer3 {
        int wordCount
        String key

        @Topic("words-records")
        void countWord(@KafkaKey String key, ConsumerRecord<String, String> record) {
            wordCount += record.value().split(/\s/).size()
            this.key = key
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaListenerSpec')
    @KafkaListener(offsetReset = EARLIEST)
    static class MyConsumer4 {
        String body

        @Topic("words")
        void countWord(@MessageBody String body) {
            this.body = body
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaListenerSpec')
    @KafkaListener(offsetReset = EARLIEST)
    static class MyConsumer5 {
        boolean missingHeader
        String sentence
        String topic

        @Topic("words")
        void countWord(@MessageBody String sentence,
                       @MessageHeader Optional<String> topic,
                       @MessageHeader Optional<String> missing) {
            missingHeader = !missing.isPresent()
            this.sentence = sentence
            this.topic = topic.get()
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaListenerSpec')
    @KafkaListener(offsetReset = EARLIEST)
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

    @Requires(property = 'spec.name', value = 'KafkaListenerSpec')
    @KafkaListener(offsetReset = EARLIEST)
    static class PojoConsumer {
        Book lastBook
        MessageHeaders messageHeaders

        @Topic("books")
        void receiveBook(@Nullable Book book, MessageHeaders messageHeaders) {
            lastBook = book
            this.messageHeaders = messageHeaders
        }
    }

    @EqualsAndHashCode
    static class Book {
        String title
    }
}
