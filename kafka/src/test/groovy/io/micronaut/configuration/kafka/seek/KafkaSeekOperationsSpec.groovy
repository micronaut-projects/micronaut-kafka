package io.micronaut.configuration.kafka.seek

import io.micronaut.configuration.kafka.AbstractKafkaContainerSpec
import io.micronaut.configuration.kafka.annotation.KafkaClient
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.context.annotation.Requires
import jakarta.inject.Singleton
import org.apache.kafka.common.TopicPartition

import java.time.Instant

import static io.micronaut.configuration.kafka.annotation.OffsetReset.EARLIEST
import static io.micronaut.configuration.kafka.config.AbstractKafkaConfiguration.EMBEDDED_TOPICS
import static java.time.temporal.ChronoUnit.DAYS

class KafkaSeekOperationsSpec extends AbstractKafkaContainerSpec {

    static final String TEST_TOPIC = "KafkaSeekOperationsSpec-messages"
    static final TopicPartition TP = new TopicPartition(TEST_TOPIC, 0)
    static final TopicPartition WRONG_TP = new TopicPartition("wrong-topic", 123)

    static final String MESSAGE_0 = "zero"
    static final String MESSAGE_1 = "one"
    static final String MESSAGE_2 = "two"
    static final String MESSAGE_3 = "three"
    static final String MESSAGE_4 = "four"
    static final String MESSAGE_5 = "five"
    static final String MESSAGE_6 = "six"
    static final String MESSAGE_7 = "seven"
    static final List<String> ALL_MESSAGES = [MESSAGE_0, MESSAGE_1, MESSAGE_2, MESSAGE_3, MESSAGE_4, MESSAGE_5, MESSAGE_6, MESSAGE_7]

    @Override
    protected Map<String, Object> getConfiguration() {
        super.configuration + ['kafka.consumers.default.max.poll.records': 1, (EMBEDDED_TOPICS): TEST_TOPIC]
    }

    @Override
    protected int getConditionsTimeout() {
        5
    }

    void "defer seek operations from a consumer method"() {
        given: "consumers that implement ConsumerSeekAware are rebalanced"
        final MyConsumer01 consumer01 = context.getBean(MyConsumer01) // absolute seek to offset 4
        final MyConsumer02 consumer02 = context.getBean(MyConsumer02) // seek to beginning
        final MyConsumer03 consumer03 = context.getBean(MyConsumer03) // seek to beginning + 2
        final MyConsumer04 consumer04 = context.getBean(MyConsumer04) // seek to end
        final MyConsumer05 consumer05 = context.getBean(MyConsumer05) // seek to end - 2
        final MyConsumer06 consumer06 = context.getBean(MyConsumer06) // seek to future timestamp
        final MyConsumer07 consumer07 = context.getBean(MyConsumer07) // seek to negative offset
        final MyConsumer08 consumer08 = context.getBean(MyConsumer08) // force seek error
        final MyConsumer09 consumer09 = context.getBean(MyConsumer09) // seek to current + 3
        final MyConsumer10 consumer10 = context.getBean(MyConsumer10) // seek to current - 3

        expect: "consumer#1 performed absolute seek operation to offset 4 -- offsets 2 to 3 are skipped"
        conditions.eventually {
            consumer01.messages == [
                    MESSAGE_0,
                    MESSAGE_1,
                    // now sek to offset 4
                    MESSAGE_4,
                    MESSAGE_5,
                    MESSAGE_6,
                    MESSAGE_7,
            ]
        }

        and: "consumer#2 performed seek-to-beginning operation -- offsets 0 to 2 are processed twice"
        conditions.eventually {
            consumer02.messages == [
                    MESSAGE_0,
                    MESSAGE_1,
                    MESSAGE_2,
                    // now seek to the beginning
                    MESSAGE_0,
                    MESSAGE_1,
                    MESSAGE_2,
                    MESSAGE_3,
                    MESSAGE_4,
                    MESSAGE_5,
                    MESSAGE_6,
                    MESSAGE_7,
            ]
        }

        and: "consumer#3 performed seek to beginning plus 2 -- offsets 2 to 4 are processed twice"
        conditions.eventually {
            consumer03.messages == [
                    MESSAGE_0,
                    MESSAGE_1,
                    MESSAGE_2,
                    MESSAGE_3,
                    MESSAGE_4,
                    // now seek to the beginning + 2
                    MESSAGE_2,
                    MESSAGE_3,
                    MESSAGE_4,
                    MESSAGE_5,
                    MESSAGE_6,
                    MESSAGE_7,
            ]
        }

        and: "consumer#4 performed seek-to-end operation -- offsets 6 to 7 are skipped"
        conditions.eventually {
            consumer04.messages == [
                    MESSAGE_0,
                    MESSAGE_1,
                    MESSAGE_2,
                    MESSAGE_3,
                    // now seek to the end
            ]
        }

        and: "consumer#5 performed seek to end minus 2 -- offsets 4 to 5 are skipped"
        conditions.eventually {
            consumer05.messages == [
                    MESSAGE_0,
                    MESSAGE_1,
                    MESSAGE_2,
                    MESSAGE_3,
                    // now seek to the end - 2
                    MESSAGE_6,
                    MESSAGE_7,
            ]
        }

        and: "consumer#6 performed seek to today plus 7 days -- falls back to seek-to-end"
        conditions.eventually {
            consumer06.messages == [
                    MESSAGE_0,
                    MESSAGE_1,
                    MESSAGE_2,
                    // now seek to today + 7 days
            ]
        }

        and: "consumer#7 performed seek to negative offset -- no offsets are skipped"
        conditions.eventually {
            consumer07.messages == ALL_MESSAGES
            consumer07.error instanceof IllegalArgumentException
        }

        and: "consumer#8 forced seek error -- no offsets are skipped"
        conditions.eventually {
            consumer08.messages == [
                    MESSAGE_0,
                    MESSAGE_1,
                    MESSAGE_2,
                    // force seek error
                    MESSAGE_3,
                    MESSAGE_4,
                    MESSAGE_5,
                    MESSAGE_6,
                    MESSAGE_7,
            ]
        }

        and: "consumer#9 performed seek to current plus 3 -- offsets 4 to 5 are skipped"
        conditions.eventually {
            consumer09.messages == [
                    MESSAGE_0,
                    MESSAGE_1,
                    MESSAGE_2,
                    MESSAGE_3,
                    // now seek to current + 3
                    MESSAGE_7,
            ]
        }

        and: "consumer#10 performed seek to current minus 3 -- offsets 3 to 6 are processed twice"
        conditions.eventually {
            consumer10.messages == [
                    MESSAGE_0,
                    MESSAGE_1,
                    MESSAGE_2,
                    MESSAGE_3,
                    MESSAGE_4,
                    MESSAGE_5,
                    MESSAGE_6,
                    // now seek to current - 3
                    MESSAGE_4,
                    MESSAGE_5,
                    MESSAGE_6,
                    MESSAGE_7,
            ]
        }
    }

    @KafkaClient
    @Requires(property = 'spec.name', value = 'KafkaSeekOperationsSpec')
    static interface MyProducer {
        @Topic(TEST_TOPIC) void produce(String message)
    }

    @Singleton
    @Requires(property = 'spec.name', value = 'KafkaSeekOperationsSpec')
    static class TestMessages {
        TestMessages(MyProducer producer) { ALL_MESSAGES.forEach(producer::produce) }
    }

    static abstract class MyAbstractConsumer {
        final List<String> messages = []
        final String targetMessage
        Exception error
        boolean alreadyDone
        MyAbstractConsumer(String message) { targetMessage = message }
        @Topic(TEST_TOPIC)
        void consume(String message, KafkaSeekOperations ops) {
            messages << message
            if (!alreadyDone && message == targetMessage) {
                alreadyDone = true
                try { ops.defer(doTheSeek()) } catch (Exception e) { error = e }
            }
        }
        abstract KafkaSeekOperation doTheSeek()
    }

    @KafkaListener(offsetReset = EARLIEST)
    @Requires(property = 'spec.name', value = 'KafkaSeekOperationsSpec')
    static class MyConsumer01 extends MyAbstractConsumer {
        MyConsumer01(TestMessages test) { super('one') }
        KafkaSeekOperation doTheSeek() { KafkaSeekOperation.seek(TP, 4) }
    }

    @KafkaListener(offsetReset = EARLIEST)
    @Requires(property = 'spec.name', value = 'KafkaSeekOperationsSpec')
    static class MyConsumer02 extends MyAbstractConsumer {
        MyConsumer02(TestMessages test) { super('two') }
        KafkaSeekOperation doTheSeek() { KafkaSeekOperation.seekToBeginning(TP) }
    }

    @KafkaListener(offsetReset = EARLIEST)
    @Requires(property = 'spec.name', value = 'KafkaSeekOperationsSpec')
    static class MyConsumer03 extends MyAbstractConsumer {
        MyConsumer03(TestMessages test) { super('four') }
        KafkaSeekOperation doTheSeek() { KafkaSeekOperation.seekRelativeToBeginning(TP, 2) }
    }

    @KafkaListener(offsetReset = EARLIEST)
    @Requires(property = 'spec.name', value = 'KafkaSeekOperationsSpec')
    static class MyConsumer04 extends MyAbstractConsumer {
        MyConsumer04(TestMessages test) { super('three') }
        KafkaSeekOperation doTheSeek() { KafkaSeekOperation.seekToEnd(TP) }
    }

    @KafkaListener(offsetReset = EARLIEST)
    @Requires(property = 'spec.name', value = 'KafkaSeekOperationsSpec')
    static class MyConsumer05 extends MyAbstractConsumer {
        MyConsumer05(TestMessages test) { super('three') }
        KafkaSeekOperation doTheSeek() { KafkaSeekOperation.seekRelativeToEnd(TP, 2) }
    }

    @KafkaListener(offsetReset = EARLIEST)
    @Requires(property = 'spec.name', value = 'KafkaSeekOperationsSpec')
    static class MyConsumer06 extends MyAbstractConsumer {
        MyConsumer06(TestMessages test) { super('two') }
        KafkaSeekOperation doTheSeek() { KafkaSeekOperation.seekToTimestamp(TP, Instant.now().plus(7, DAYS).toEpochMilli()) }
    }

    @KafkaListener(offsetReset = EARLIEST)
    @Requires(property = 'spec.name', value = 'KafkaSeekOperationsSpec')
    static class MyConsumer07 extends MyAbstractConsumer {
        MyConsumer07(TestMessages test) { super('five') }
        KafkaSeekOperation doTheSeek() { KafkaSeekOperation.seek(TP, -5) }
    }

    @KafkaListener(offsetReset = EARLIEST)
    @Requires(property = 'spec.name', value = 'KafkaSeekOperationsSpec')
    static class MyConsumer08 extends MyAbstractConsumer {
        MyConsumer08(TestMessages test) { super('two') }
        KafkaSeekOperation doTheSeek() { KafkaSeekOperation.seek(WRONG_TP, 321) }
    }

    @KafkaListener(offsetReset = EARLIEST)
    @Requires(property = 'spec.name', value = 'KafkaSeekOperationsSpec')
    static class MyConsumer09 extends MyAbstractConsumer {
        MyConsumer09(TestMessages test) { super('three') }
        KafkaSeekOperation doTheSeek() { KafkaSeekOperation.seekForward(TP, 3) }
    }

    @KafkaListener(offsetReset = EARLIEST)
    @Requires(property = 'spec.name', value = 'KafkaSeekOperationsSpec')
    static class MyConsumer10 extends MyAbstractConsumer {
        MyConsumer10(TestMessages test) { super('six') }
        KafkaSeekOperation doTheSeek() { KafkaSeekOperation.seekBackward(TP, 3) }
    }
}
