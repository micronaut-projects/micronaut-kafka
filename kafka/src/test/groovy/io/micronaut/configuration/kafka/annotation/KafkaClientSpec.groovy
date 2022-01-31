package io.micronaut.configuration.kafka.annotation

import io.micronaut.configuration.kafka.AbstractKafkaSpec
import io.micronaut.context.ApplicationContext
import io.micronaut.context.annotation.Property
import io.micronaut.context.annotation.Requires
import io.micronaut.core.io.socket.SocketUtils
import io.micronaut.messaging.exceptions.MessagingClientException
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.header.internals.RecordHeaders
import reactor.core.publisher.Mono
import spock.lang.AutoCleanup

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
}
