package io.micronaut.configuration.kafka.annotation


import io.micronaut.configuration.kafka.ConsumerRegistry
import io.micronaut.context.ApplicationContext
import io.micronaut.core.util.CollectionUtils
import io.micronaut.http.client.RxHttpClient
import io.micronaut.messaging.annotation.Header
import io.micronaut.runtime.server.EmbeddedServer
import org.apache.kafka.clients.consumer.Consumer
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification

class KafkaDisabledSpec extends Specification {

    @Shared
    @AutoCleanup
    ApplicationContext context
    @Shared
    @AutoCleanup
    EmbeddedServer embeddedServer

    def setupSpec() {
        embeddedServer = ApplicationContext.run(EmbeddedServer,
                CollectionUtils.mapOf(
                        "kafka.enabled", false
                )
        )
        context = embeddedServer.applicationContext
    }

    void "test simple consumer"() {
        expect: "No kafka consumers have been created"
        !context.containsBean(ConsumerRegistry)
    }

    void "test simple producer"() {
        given:
        MyClient myClient = context.getBean(MyClient)

        when:
        myClient.sendSentence("abcd", "asdf", "words")

        then:
        noExceptionThrown()
    }

    @KafkaListener(offsetReset = OffsetReset.EARLIEST)
    static class MyConsumer {
        int wordCount
        String lastTopic

        @Topic("words")
        void receive(String sentence, @Header String topic) {
            wordCount += sentence.split(/\s/).size()
            lastTopic = topic
        }
    }

    @KafkaClient
    static interface MyClient {
        @Topic("words")
        void sendSentence(@KafkaKey String key, String sentence, @Header String topic)
    }

}
