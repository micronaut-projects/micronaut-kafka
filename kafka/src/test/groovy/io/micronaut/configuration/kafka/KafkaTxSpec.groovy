package io.micronaut.configuration.kafka

import io.micronaut.configuration.kafka.annotation.ErrorStrategy
import io.micronaut.configuration.kafka.annotation.ErrorStrategyValue
import io.micronaut.configuration.kafka.annotation.KafkaClient
import io.micronaut.configuration.kafka.annotation.KafkaKey
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.OffsetReset
import io.micronaut.configuration.kafka.annotation.OffsetStrategy
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.context.annotation.Requires
import io.micronaut.core.type.Argument
import io.micronaut.messaging.annotation.SendTo
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.IsolationLevel

import java.util.stream.Collectors
import java.util.stream.Stream

class KafkaTxSpec extends AbstractKafkaContainerSpec {

    protected Map<String, Object> getConfiguration() {
        super.configuration + [
                'kafka.producers.tx-word-counter.key.serializer'  : 'org.apache.kafka.common.serialization.StringSerializer',
                'kafka.producers.tx-word-counter.value.serializer': 'org.apache.kafka.common.serialization.IntegerSerializer'
        ]
    }

    def "should count words in transaction"() {
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

    def "should not receive messages when tx is rollbacked"() {
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
            Thread.sleep(5000)
            numbersProducer.abortTransaction()
        then:
            conditions.eventually {
                committedNumbersCollector.numbers.size() == 0
                uncommittedNumbersCollector.numbers.size() >= 20
            }
    }

    def "should not receive same numbers"() {
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

    def "should not receive same numbers2"() {
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

    @Requires(property = 'spec.name', value = 'KafkaTxSpec')
    @KafkaListener(isolation = IsolationLevel.READ_COMMITTED,
            offsetReset = OffsetReset.EARLIEST,
            offsetStrategy = OffsetStrategy.SYNC,
            errorStrategy = @ErrorStrategy(value = ErrorStrategyValue.RETRY_ON_ERROR))
    static class IdempotenceNumbersCollector {

        List<Integer> numbers = new ArrayList<>()
        Set<Integer> failed = new HashSet<>()

        @Topic("tx-idempotence-numbers")
        void collect(int number) {
            if (number % 2 == 0) {
                if (failed.add(number)) {
                    throw new IllegalAccessException()
                }
            }
            numbers.add(number)
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaTxSpec')
    @KafkaListener(isolation = IsolationLevel.READ_COMMITTED,
            offsetReset = OffsetReset.EARLIEST,
            offsetStrategy = OffsetStrategy.SYNC,
            errorStrategy = @ErrorStrategy(value = ErrorStrategyValue.RETRY_ON_ERROR, retryDelay = ""))
    static class IdempotenceNumbersNoDelayCollector {

        List<Integer> numbers = new ArrayList<>()
        Set<Integer> failed = new HashSet<>()

        @Topic("tx-idempotence-numbers")
        void collect(int number) {
            if (number % 2 == 0) {
                if (failed.add(number)) {
                    throw new IllegalAccessException()
                }
            }
            numbers.add(number)
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaTxSpec')
    @KafkaClient(transactionalId = "tx-idempotence-number-producer")
    static interface IdempotenceNumbersProducer {

        @Topic("tx-idempotence-numbers")
        void send(@KafkaKey String id, int number);
    }

    @Requires(property = 'spec.name', value = 'KafkaTxSpec')
    @KafkaListener(isolation = IsolationLevel.READ_COMMITTED)
    static class CommittedNumbersCollector {

        List<Integer> numbers = new ArrayList<>()

        @Topic("tx-numbers")
        void collect(int number) {
            numbers.add(number)
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaTxSpec')
    @KafkaListener(isolation = IsolationLevel.READ_UNCOMMITTED)
    static class UncommittedNumbersCollector {

        List<Integer> numbers = new ArrayList<>()

        @Topic("tx-numbers")
        void collect(int number) {
            numbers.add(number)
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaTxSpec')
    @KafkaListener(isolation = IsolationLevel.READ_COMMITTED)
    static class WordCountCollector {

        Map<String, Integer> counter = new HashMap<>()

        @Topic("tx-words-count")
        void count(@KafkaKey String word, int count) {
            counter.put(word, count)
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaTxSpec')
    @KafkaListener(producerClientId = "tx-word-counter",
            producerTransactionalId = "tx-word-counter",
            offsetStrategy = OffsetStrategy.SEND_TO_TRANSACTION, isolation = IsolationLevel.READ_COMMITTED)
    static class WordCounter {

        @Topic("tx-strings")
        @SendTo("tx-words-count")
        List<KafkaMessage> wordsCounter(String string) {
            Map<String, Integer> wordsCount = Stream.of(string.split(" "))
                    .map(word -> new AbstractMap.SimpleEntry<>(word, 1))
                    .collect(Collectors.toMap((Map.Entry<String, Integer> e) -> e.key, (Map.Entry<String, Integer> e) -> e.value, (v1, v2) -> v1 + v2))
            List<KafkaMessage> messages = new ArrayList<>()
            for (Map.Entry<String, Integer> e : wordsCount) {
                messages.add(KafkaMessage.Builder.withBody(e.getValue()).key(e.getKey()).build())
            }
            return messages
        }

    }

    @Requires(property = 'spec.name', value = 'KafkaTxSpec')
    @KafkaClient(transactionalId = "tx-string-producer")
    static interface StringProducer {

        @Topic("tx-strings")
        void send(String strings);
    }

}