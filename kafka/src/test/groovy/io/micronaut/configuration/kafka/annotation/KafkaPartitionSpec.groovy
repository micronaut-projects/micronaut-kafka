package io.micronaut.configuration.kafka.annotation

import io.micronaut.configuration.kafka.AbstractEmbeddedServerSpec
import io.micronaut.context.annotation.Requires

import java.util.concurrent.ConcurrentHashMap

import static io.micronaut.configuration.kafka.annotation.KafkaClient.Acknowledge.ALL

class KafkaPartitionSpec extends AbstractEmbeddedServerSpec {

    public static final String TOPIC_WORDS = "KafkaPartitionSpec-words"

    @Override
    void afterKafkaStarted() {
        createTopic(TOPIC_WORDS, 3, 1)
        sleep 5_000
    }

    Map<String, Object> getConfiguration() {
        super.configuration +
                ["kafka.consumers.kafka-partition-group.allow.auto.create.topics" : false]
    }

    void "test client without partition"() {
        given:
        ClientWithoutPartition client = context.getBean(ClientWithoutPartition)
        SentenceListener listener = context.getBean(SentenceListener)
        listener.entries.clear()

        when:
        client.sendSentence("key1", "sentence1")
        client.sendSentence("key2", "sentence2")
        client.sendSentence("key3", "sentence3")

        then:
        conditions.eventually {
            listener.entries.size() == 3
            listener.entries["sentence1"] == 2 // "key1" happens to result in this
            listener.entries["sentence2"] == 2 // "key2" happens to result in this
            listener.entries["sentence3"] == 1 // "key3" happens to result in this
        }
    }

    void "test client with integer partition"() {
        given:
        ClientWithIntegerPartition client = context.getBean(ClientWithIntegerPartition)
        SentenceListener listener = context.getBean(SentenceListener)
        listener.entries.clear()

        when:
        client.sendSentence(1, "key1", "sentence1")
        client.sendSentence(2, "key2", "sentence2")
        client.sendSentence(2, "key3", "sentence3")

        then:
        conditions.eventually {
            listener.entries.size() == 3
            listener.entries["sentence1"] == 1
            listener.entries["sentence2"] == 2
            listener.entries["sentence3"] == 2
        }
    }

    void "test client with integer partition null"() {
        given:
        ClientWithIntegerPartition client = context.getBean(ClientWithIntegerPartition)
        SentenceListener listener = context.getBean(SentenceListener)
        listener.entries.clear()

        when:
        client.sendSentence(null, "key1", "sentence1")
        client.sendSentence(null, "key2", "sentence2")
        client.sendSentence(null, "key3", "sentence3")

        then:
        conditions.eventually {
            listener.entries.size() == 3
            listener.entries["sentence1"] == 2 // "key1" happens to result in this
            listener.entries["sentence2"] == 2 // "key2" happens to result in this
            listener.entries["sentence3"] == 1 // "key3" happens to result in this
        }
    }

    void "test client with int partition"() {
        given:
        ClientWithIntPartition client = context.getBean(ClientWithIntPartition)
        SentenceListener listener = context.getBean(SentenceListener)
        listener.entries.clear()

        when:
        client.sendSentence(1, "key1", "sentence1")
        client.sendSentence(2, "key2", "sentence2")
        client.sendSentence(2, "key3", "sentence3")

        then:
        conditions.eventually {
            listener.entries.size() == 3
            listener.entries["sentence1"]  == 1
            listener.entries["sentence2"]  == 2
            listener.entries["sentence3"]  == 2
        }
    }

    void "test client with partition key"() {
        given:
        ClientWithPartitionKey client = context.getBean(ClientWithPartitionKey)
        SentenceListener listener = context.getBean(SentenceListener)
        listener.entries.clear()

        when:
        client.sendSentence("par-key1", "key1", "sentence1")
        client.sendSentence("par-key2", "key2", "sentence2")
        client.sendSentence("par-key3", "key3", "sentence3")

        then:
        conditions.eventually {
            listener.entries.size() == 3
            listener.entries["sentence1"] == 2 // "par-key1" happens to result in this
            listener.entries["sentence2"] == 0 // "par-key2" happens to result in this
            listener.entries["sentence3"] == 2 // "par-key3" happens to result in this
        }
    }

    void "test client with partition key null"() {
        given:
        ClientWithPartitionKey client = context.getBean(ClientWithPartitionKey)
        SentenceListener listener = context.getBean(SentenceListener)
        listener.entries.clear()

        when:
        client.sendSentence(null, "key1", "sentence1")
        client.sendSentence(null, "key2", "sentence2")
        client.sendSentence(null, "key3", "sentence3")

        then:
        conditions.eventually {
            listener.entries.size() == 3
            listener.entries["sentence1"] == 2 // "key1" happens to result in this
            listener.entries["sentence2"] == 2 // "key2" happens to result in this
            listener.entries["sentence3"] == 1 // "key3" happens to result in this
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaPartitionSpec')
    @KafkaClient(acks = ALL)
    static interface ClientWithoutPartition {
        @Topic(KafkaPartitionSpec.TOPIC_WORDS)
        void sendSentence(@KafkaKey String key, String sentence)
    }

    @Requires(property = 'spec.name', value = 'KafkaPartitionSpec')
    @KafkaClient(acks = ALL)
    static interface ClientWithIntPartition {
        @Topic(KafkaPartitionSpec.TOPIC_WORDS)
        void sendSentence(@KafkaPartition int partition, @KafkaKey String key, String sentence)
    }

    @Requires(property = 'spec.name', value = 'KafkaPartitionSpec')
    @KafkaClient(acks = ALL)
    static interface ClientWithIntegerPartition {
        @Topic(KafkaPartitionSpec.TOPIC_WORDS)
        void sendSentence(@KafkaPartition Integer partition, @KafkaKey String key, String sentence)
    }

    @Requires(property = 'spec.name', value = 'KafkaPartitionSpec')
    @KafkaClient(acks = ALL)
    static interface ClientWithPartitionKey {
        @Topic(KafkaPartitionSpec.TOPIC_WORDS)
        void sendSentence(@KafkaPartitionKey String partitionKey, @KafkaKey String key, String sentence)
    }

    @Requires(property = 'spec.name', value = 'KafkaPartitionSpec')
    @KafkaListener(value = "kafka-partition-group")
    static class SentenceListener {
        Map<String, Integer> entries = new ConcurrentHashMap<>()

        @Topic(KafkaPartitionSpec.TOPIC_WORDS)
        void receive(@KafkaPartition int partition, String sentence) {
            entries[sentence] = partition
        }
    }
}
