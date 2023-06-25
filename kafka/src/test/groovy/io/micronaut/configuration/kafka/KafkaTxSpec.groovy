package io.micronaut.configuration.kafka

import io.micronaut.configuration.kafka.annotation.ErrorStrategy
import io.micronaut.configuration.kafka.annotation.KafkaClient
import io.micronaut.configuration.kafka.annotation.KafkaKey
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.context.annotation.Requires
import io.micronaut.core.type.Argument
import io.micronaut.messaging.annotation.SendTo
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.IntegerSerializer
import org.apache.kafka.common.serialization.StringSerializer

import java.util.stream.Collectors
import java.util.stream.Stream

import static io.micronaut.configuration.kafka.annotation.ErrorStrategyValue.RETRY_ON_ERROR
import static io.micronaut.configuration.kafka.annotation.OffsetReset.EARLIEST
import static io.micronaut.configuration.kafka.annotation.OffsetStrategy.SEND_TO_TRANSACTION
import static io.micronaut.configuration.kafka.annotation.OffsetStrategy.SYNC
import static org.apache.kafka.common.IsolationLevel.READ_COMMITTED
import static org.apache.kafka.common.IsolationLevel.READ_UNCOMMITTED

class KafkaTxSpec extends AbstractKafkaContainerSpec {

    protected Map<String, Object> getConfiguration() {
        super.configuration + [
                'kafka.producers.tx-word-counter.key.serializer'  : StringSerializer.name,
                'kafka.producers.tx-word-counter.value.serializer': IntegerSerializer.name
        ]
    }

    void "should count words in transaction"() {
        given:
        StringProducer stringProducer = context.getBean(StringProducer)
        WordCountCollector wordCountCollector = context.getBean(WordCountCollector)

        when:
        stringProducer.send("The quick brown fox jumps over the lazy dog. THE QUICK BROWN FOX JUMPED OVER THE LAZY DOG'S BACK")

        then:
        conditions.eventually {
            wordCountCollector.counter['quick'] == 1
            wordCountCollector.counter['THE'] == 2
            wordCountCollector.counter.size() == 18
        }
    }

    void "should not receive messages when tx is rolled back"() {
        given:
        TransactionalProducerRegistry transactionalProducerRegistry = context.getBean(TransactionalProducerRegistry)
        def numbersProducer = transactionalProducerRegistry.getTransactionalProducer(null, "tx-nid-producer", Argument.of(String), Argument.of(Integer))
        CommittedNumbersCollector committedNumbersCollector = context.getBean(CommittedNumbersCollector)
        UncommittedNumbersCollector uncommittedNumbersCollector = context.getBean(UncommittedNumbersCollector)

        when:
        numbersProducer.beginTransaction()
        (1..20).forEach(it -> {
            numbersProducer.send(new ProducerRecord<String, Integer>("tx-numbers", it))
        })
        sleep 5_000
        numbersProducer.abortTransaction()

        then:
        conditions.eventually {
            committedNumbersCollector.numbers.size() == 0
            uncommittedNumbersCollector.numbers.size() >= 20
        }
    }

    void "should not receive same numbers"() {
        given:
        TransactionalProducerRegistry transactionalProducerRegistry = context.getBean(TransactionalProducerRegistry)
        def numbersProducer = transactionalProducerRegistry.getTransactionalProducer("tx-nid-producer", "tx-nid-producer", Argument.of(String), Argument.of(Integer))
        IdempotenceNumbersCollector idempotenceNumbersCollector = context.getBean(IdempotenceNumbersCollector)
        IdempotenceNumbersNoDelayCollector idempotenceNumbersNoDelayCollector = context.getBean(IdempotenceNumbersNoDelayCollector)

        when:
        idempotenceNumbersCollector.numbers.clear()
        idempotenceNumbersCollector.failed.clear()
        numbersProducer.beginTransaction()
        (1..30).forEach(it -> {
            numbersProducer.send(new ProducerRecord<String, Integer>("tx-idempotence-numbers", it.toString(), it))
        })
        numbersProducer.commitTransaction()

        then:
        conditions.eventually {
            idempotenceNumbersCollector.numbers.size() == 30
            idempotenceNumbersNoDelayCollector.numbers.size() == 30
        }
    }

    void "should not receive same numbers2"() {
        given:
        def numbersProducer = context.getBean(IdempotenceNumbersProducer)
        IdempotenceNumbersCollector idempotenceNumbersCollector = context.getBean(IdempotenceNumbersCollector)

        when:
        idempotenceNumbersCollector.numbers.clear()
        idempotenceNumbersCollector.failed.clear()
        (1..30).forEach(it -> {
            numbersProducer.send(it.toString(), it)
        })

        then:
        conditions.eventually {
            idempotenceNumbersCollector.numbers.size() == 30
        }
    }

    void "should create producer per thread"() {
        given:
        Producer firstNumbersProducer
        Producer secondNumbersProducer
        TransactionalProducerRegistry transactionalProducerRegistry = context.getBean(TransactionalProducerRegistry)

        when:
        def one = Thread.start {
            firstNumbersProducer = transactionalProducerRegistry.getTransactionalProducer("tx-nid-producer", "tx-nid-producer", Argument.of(String), Argument.of(Integer))
        }

        def second = Thread.start {
            secondNumbersProducer = transactionalProducerRegistry.getTransactionalProducer("tx-nid-producer", "tx-nid-producer", Argument.of(String), Argument.of(Integer))
        }

        sleep 5_000

        then:
        one.join()
        second.join()
    }

    void "should throw exception when producers limit"() {

    }

    @Requires(property = 'spec.name', value = 'KafkaTxSpec')
    @KafkaListener(
            isolation = READ_COMMITTED,
            offsetReset = EARLIEST,
            offsetStrategy = SYNC,
            errorStrategy = @ErrorStrategy(value = RETRY_ON_ERROR))
    static class IdempotenceNumbersCollector {

        List<Integer> numbers = []
        Set<Integer> failed = []

        @Topic("tx-idempotence-numbers")
        void collect(int number) {
            if (number % 2 == 0) {
                if (failed.add(number)) {
                    throw new IllegalAccessException()
                }
            }
            numbers << number
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaTxSpec')
    @KafkaListener(
            isolation = READ_COMMITTED,
            offsetReset = EARLIEST,
            offsetStrategy = SYNC,
            errorStrategy = @ErrorStrategy(value = RETRY_ON_ERROR, retryDelay = ""))
    static class IdempotenceNumbersNoDelayCollector {

        List<Integer> numbers = []
        Set<Integer> failed = []

        @Topic("tx-idempotence-numbers")
        void collect(int number) {
            if (number % 2 == 0) {
                if (failed.add(number)) {
                    throw new IllegalAccessException()
                }
            }
            numbers << number
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaTxSpec')
    @KafkaClient(transactionalId = "tx-idempotence-number-producer")
    static interface IdempotenceNumbersProducer {

        @Topic("tx-idempotence-numbers")
        void send(@KafkaKey String id, int number)
    }

    @Requires(property = 'spec.name', value = 'KafkaTxSpec')
    @KafkaListener(isolation = READ_COMMITTED)
    static class CommittedNumbersCollector {

        List<Integer> numbers = []

        @Topic("tx-numbers")
        void collect(int number) {
            numbers << number
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaTxSpec')
    @KafkaListener(isolation = READ_UNCOMMITTED)
    static class UncommittedNumbersCollector {

        List<Integer> numbers = []

        @Topic("tx-numbers")
        void collect(int number) {
            numbers << number
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaTxSpec')
    @KafkaListener(isolation = READ_COMMITTED)
    static class WordCountCollector {

        Map<String, Integer> counter = [:]

        @Topic("tx-words-count")
        void count(@KafkaKey String word, int count) {
            counter[word] = count
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaTxSpec')
    @KafkaListener(
            producerClientId = "tx-word-counter",
            producerTransactionalId = "tx-word-counter",
            offsetStrategy = SEND_TO_TRANSACTION, isolation = READ_COMMITTED)
    static class WordCounter {

        @Topic("tx-strings")
        @SendTo("tx-words-count")
        List<KafkaMessage> wordsCounter(String string) {
            Map<String, Integer> wordsCount = Stream.of(string.split(" "))
                    .map(word -> new AbstractMap.SimpleEntry<>(word, 1))
                    .collect(Collectors.toMap((Map.Entry<String, Integer> e) -> e.key, (Map.Entry<String, Integer> e) -> e.value, (v1, v2) -> v1 + v2))
            List<KafkaMessage> messages = []
            for (Map.Entry<String, Integer> e : wordsCount) {
                messages << KafkaMessage.Builder.withBody(e.value).key(e.key).build()
            }
            return messages
        }

    }

    @Requires(property = 'spec.name', value = 'KafkaTxSpec')
    @KafkaClient(transactionalId = "tx-string-producer")
    static interface StringProducer {
        @Topic("tx-strings")
        void send(String strings)
    }
}
