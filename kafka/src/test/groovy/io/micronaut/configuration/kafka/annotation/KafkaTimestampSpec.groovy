
package io.micronaut.configuration.kafka.annotation

import io.micronaut.configuration.kafka.config.AbstractKafkaConfiguration
import io.micronaut.context.ApplicationContext
import io.micronaut.core.util.CollectionUtils
import org.apache.kafka.clients.producer.ProducerRecord
import org.testcontainers.containers.KafkaContainer
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification
import spock.util.concurrent.PollingConditions

import java.util.concurrent.ConcurrentLinkedDeque

class KafkaTimestampSpec extends Specification {

    public static final String TOPIC_WORDS = "KafkaTimestampSpec-words"

    @Shared @AutoCleanup KafkaContainer kafkaContainer = new KafkaContainer()
    @Shared
    @AutoCleanup
    ApplicationContext context

    def setupSpec() {
        kafkaContainer.start()
        context = ApplicationContext.run(
                CollectionUtils.mapOf(
                        "kafka.bootstrap.servers", kafkaContainer.getBootstrapServers(),
                        AbstractKafkaConfiguration.EMBEDDED_TOPICS,
                        [TOPIC_WORDS]
                )
        )
    }

    def "test client without timestamp"() {
        given:
        ClientWithoutClientTimestamp client = context.getBean(ClientWithoutClientTimestamp)
        SentenceListener listener = context.getBean(SentenceListener)
        listener.keys.clear()
        listener.sentences.clear()
        listener.timestamps.clear()
        PollingConditions conditions = new PollingConditions(timeout: 30, delay: 1)
        ProducerRecord pr = Spy(ProducerRecord, constructorArgs: [TOPIC_WORDS, null, null, "key", "sentence", new ArrayList()]) as ProducerRecord

        when:
        client.sendSentence("key", "sentence")

        then:
        conditions.eventually {
            listener.keys.size() == 1
            listener.keys.iterator().next() == "key"
            listener.sentences.size() == 1
            listener.sentences.iterator().next() == "sentence"
            listener.timestamps.size() == 1
            listener.timestamps.iterator().next() != null
        }

    }

    def "test client with timestamp"() {
        given:
        ClientWithClientTimestamp client = context.getBean(ClientWithClientTimestamp)
        SentenceListener listener = context.getBean(SentenceListener)
        listener.keys.clear()
        listener.sentences.clear()
        listener.timestamps.clear()
        PollingConditions conditions = new PollingConditions(timeout: 30, delay: 1)

        when:
        client.sendSentence("key", "sentence")

        then:
        conditions.eventually {
            listener.keys.size() == 1
            listener.keys.iterator().next() == "key"
            listener.sentences.size() == 1
            listener.sentences.iterator().next() == "sentence"
            listener.timestamps.size() == 1
            listener.timestamps.iterator().next() != null
        }

    }

    def "test client with custom timestamp"() {
        given:
        ClientWithTimestampAsParameter client = context.getBean(ClientWithTimestampAsParameter)
        SentenceListener listener = context.getBean(SentenceListener)
        listener.keys.clear()
        listener.sentences.clear()
        listener.timestamps.clear()
        PollingConditions conditions = new PollingConditions(timeout: 30, delay: 1)

        when:
        client.sendSentence("key", "sentence", 111111)

        then:
        conditions.eventually {
            listener.keys.size() == 1
            listener.keys.iterator().next() == "key"
            listener.sentences.size() == 1
            listener.sentences.iterator().next() == "sentence"
            listener.timestamps.size() == 1
            listener.timestamps.iterator().next() == 111111
        }
    }

    def "test client with timestamp and custom timestamp as parameter"() {
        given:
        ClientWithClientTimestampAndTimestampAsParameter client = context.getBean(ClientWithClientTimestampAndTimestampAsParameter)
        SentenceListener listener = context.getBean(SentenceListener)
        listener.keys.clear()
        listener.sentences.clear()
        listener.timestamps.clear()
        PollingConditions conditions = new PollingConditions(timeout: 30, delay: 1)

        when:
        client.sendSentence("key", "sentence", 111111)

        then:
        conditions.eventually {
            listener.keys.size() == 1
            listener.keys.iterator().next() == "key"
            listener.sentences.size() == 1
            listener.sentences.iterator().next() == "sentence"
            listener.timestamps.size() == 1
            listener.timestamps.iterator().next() != 111111
        }
    }

    @KafkaClient(timestamp = false)
    static interface ClientWithoutClientTimestamp {
        @Topic(KafkaTimestampSpec.TOPIC_WORDS)
        void sendSentence(@KafkaKey String key, String sentence)
    }

    @KafkaClient(timestamp = true)
    static interface ClientWithClientTimestamp {
        @Topic(KafkaTimestampSpec.TOPIC_WORDS)
        void sendSentence(@KafkaKey String key, String sentence)
    }

    @KafkaClient
    static interface ClientWithTimestampAsParameter {
        @Topic(KafkaTimestampSpec.TOPIC_WORDS)
        void sendSentence(@KafkaKey String key, String sentence, @KafkaTimestamp Long timestamp)
    }

    @KafkaClient(timestamp = true)
    static interface ClientWithClientTimestampAndTimestampAsParameter {
        @Topic(KafkaTimestampSpec.TOPIC_WORDS)
        void sendSentence(@KafkaKey String key, String sentence, @KafkaTimestamp Long timestamp)
    }

    @KafkaListener(offsetReset = OffsetReset.EARLIEST)
    static class SentenceListener {
        Queue<String> keys = new ConcurrentLinkedDeque<>()
        Queue<String> sentences = new ConcurrentLinkedDeque<>()
        Queue<Long> timestamps = new ConcurrentLinkedDeque<>()

        @Topic(KafkaTimestampSpec.TOPIC_WORDS)
        void receive(@KafkaKey String key, String sentence, Long timestamp) {
            keys << key
            sentences << sentence
            timestamps << timestamp
        }
    }

}