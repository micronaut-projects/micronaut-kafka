package io.micronaut.configuration.kafka.annotation

import io.micronaut.configuration.kafka.AbstractKafkaSpec
import io.micronaut.context.ApplicationContext
import io.micronaut.context.annotation.Property
import io.micronaut.context.annotation.Requires
import io.micronaut.core.io.socket.SocketUtils
import io.micronaut.messaging.exceptions.MessagingClientException
import io.opentracing.contrib.kafka.TracingKafkaProducer
import jakarta.inject.Singleton
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.header.internals.RecordHeaders
import reactor.core.publisher.Mono
import spock.lang.AutoCleanup

import java.lang.reflect.Field
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutionException

import static io.micronaut.configuration.kafka.annotation.KafkaClient.Acknowledge.ALL
import static io.micronaut.core.io.socket.SocketUtils.LOCALHOST
import static java.util.concurrent.TimeUnit.SECONDS

class KafkaClientSpec extends AbstractKafkaSpec {

    @AutoCleanup
    private ApplicationContext ctx

    void setup() {
        ctx = ApplicationContext.run(
                getConfiguration() +
                ['kafka.bootstrap.servers': LOCALHOST + ':' + SocketUtils.findAvailableTcpPort()])
    }

    void "test send message when Kafka is not available"() {
        given:
        MyClient client = ctx.getBean(MyClient)

        when:
        client.sendSync("test", "hello-world", [new RecordHeader("hello", "world".bytes)])

        then:
        thrown(MessagingClientException)
    }

    void "test reactive send message when Kafka is not available"() {
        given:
        MyClient client = ctx.getBean(MyClient)

        when:
        client.sendRx("test", "hello-world", new RecordHeaders([new RecordHeader("hello", "world".bytes)])).block()

        then:
        thrown(MessagingClientException)
    }

    void "test future send message when Kafka is not available"() {
        given:
        MyClient client = ctx.getBean(MyClient)

        when:
        client.sendSentence("test", "hello-world").get(1, SECONDS)

        then:
        ExecutionException e = thrown()
        e.cause instanceof MessagingClientException
    }

    void "test kafka client annotation configuration on injected kafka producer"() {
        given:
        MySender sender = ctx.getBean(MySender)

        when:
        Field producerField = TracingKafkaProducer.class.getDeclaredField("producer")
        producerField.setAccessible(true)
        KafkaProducer kafkaProducer = (KafkaProducer) producerField.get(sender.kafkaProducer)

        Field producerConfigField = KafkaProducer.class.getDeclaredField("producerConfig")
        producerConfigField.setAccessible(true)
        ProducerConfig producerConfig = (ProducerConfig) producerConfigField.get(kafkaProducer)

        Map<String, Object> config = producerConfig.originals()

        then:
        config.get(ProducerConfig.CLIENT_ID_CONFIG) == "injected-producer"
        config.get(ProducerConfig.MAX_BLOCK_MS_CONFIG) == "35000"
        config.get(ProducerConfig.ACKS_CONFIG) == "0"
        config.get(ProducerConfig.RETRIES_CONFIG) == "5"
        config.get(ProducerConfig.MAX_REQUEST_SIZE_CONFIG) == "8000"
    }

    @Requires(property = 'spec.name', value = 'KafkaClientSpec')
    @KafkaClient(maxBlock = '1s', acks = ALL)
    static interface MyClient {

        @Topic("words")
        CompletableFuture<String> sendSentence(@KafkaKey String key, String sentence)

        @Topic("words")
        @KafkaClient(
                properties = [
                        @Property(name = ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                                  value = "org.apache.kafka.common.serialization.ByteArraySerializer"),
                        @Property(name = ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                                value = "org.apache.kafka.common.serialization.ByteArraySerializer")
                ]
        )
        String sendSync(@KafkaKey String key, String sentence, Collection<Header> headers)

        @Topic("words")
        Mono<String> sendRx(@KafkaKey String key, String sentence, Headers headers)
    }

    @Singleton
    static class MySender {

        final Producer<String, String> kafkaProducer

        MySender(@KafkaClient(
                id="injected-producer",
                maxBlock="35s",
                acks = KafkaClient.Acknowledge.NONE,
                properties = [
                        @Property(name = ProducerConfig.RETRIES_CONFIG, value = "5"),
                        @Property(name = ProducerConfig.MAX_REQUEST_SIZE_CONFIG, value = "8000")
                ]) Producer<String, String> kafkaProducer) {
            this.kafkaProducer = kafkaProducer
        }

    }
}
