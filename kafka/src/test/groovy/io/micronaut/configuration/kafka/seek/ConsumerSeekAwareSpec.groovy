package io.micronaut.configuration.kafka.seek

import io.micronaut.configuration.kafka.AbstractKafkaContainerSpec
import io.micronaut.configuration.kafka.ConsumerSeekAware
import io.micronaut.configuration.kafka.annotation.KafkaClient
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.context.annotation.Requires
import jakarta.inject.Singleton
import org.apache.kafka.common.TopicPartition
import java.time.Instant

import static java.time.temporal.ChronoUnit.DAYS
import static io.micronaut.configuration.kafka.annotation.OffsetReset.EARLIEST
import static io.micronaut.configuration.kafka.annotation.OffsetReset.LATEST
import static io.micronaut.configuration.kafka.config.AbstractKafkaConfiguration.EMBEDDED_TOPICS

class ConsumerSeekAwareSpec extends AbstractKafkaContainerSpec {

    static final String TEST_TOPIC = "ConsumerSeekAwareSpec-messages"

    static final String MESSAGE_0 = "zero"
    static final String MESSAGE_1 = "one"
    static final String MESSAGE_2 = "two"
    static final String MESSAGE_3 = "three"
    static final String MESSAGE_4 = "four"
    static final String MESSAGE_5 = "five"
    static final String MESSAGE_6 = "six"
    static final String MESSAGE_7 = "seven"
    static final List<String> MESSAGES_PRE_REBALANCE = [MESSAGE_0, MESSAGE_1, MESSAGE_2, MESSAGE_3, MESSAGE_4]
    static final List<String> MESSAGES_POST_REBALANCE = [MESSAGE_5, MESSAGE_6, MESSAGE_7]
    static final List<String> ALL_MESSAGES = MESSAGES_PRE_REBALANCE + MESSAGES_POST_REBALANCE

    @Override
    protected Map<String, Object> getConfiguration() {
        super.configuration + [(EMBEDDED_TOPICS): TEST_TOPIC]
    }

    void "perform seek operations on partitions assigned"() {
        given: "consumers that implement ConsumerSeekAware are rebalanced"
        final MyConsumer01 consumer01 = context.getBean(MyConsumer01) // absolute seek to offset 3
        final MyConsumer02 consumer02 = context.getBean(MyConsumer02) // seek to beginning
        final MyConsumer03 consumer03 = context.getBean(MyConsumer03) // seek to beginning + 2
        final MyConsumer04 consumer04 = context.getBean(MyConsumer04) // seek to end
        final MyConsumer05 consumer05 = context.getBean(MyConsumer05) // seek to end - 2
        final MyConsumer06 consumer06 = context.getBean(MyConsumer06) // seek to future timestamp
        final MyConsumer07 consumer07 = context.getBean(MyConsumer07) // seek to past timestamp
        final MyConsumer08 consumer08 = context.getBean(MyConsumer08) // seek to current + 3
        final MyConsumer09 consumer09 = context.getBean(MyConsumer09) // seek to current - 3
        final MyConsumer10 consumer10 = context.getBean(MyConsumer10) // seek to current + 0

        expect: "consumers start consuming messages"
        conditions.eventually {
            !consumer01.revoked
            !consumer01.messages.empty
            !consumer02.messages.empty
            !consumer03.messages.empty
            consumer04.messages.empty
            !consumer05.messages.empty
            consumer06.messages.empty
            !consumer07.messages.empty
            !consumer08.messages.empty
            !consumer09.messages.empty
            !consumer10.messages.empty
        }

        and: "A few more messages are produced after rebalance"
        final MyProducer producer = context.getBean(MyProducer)
        MESSAGES_POST_REBALANCE.forEach(producer::produce)
        consumer01.onPartitionsLost([])

        and: "consumer#1 performed absolute seek operation to offset 3 -- offset 0 to 2 are skipped"
        conditions.eventually {
            consumer01.revoked
            consumer01.messages == [
                    MESSAGE_3,
                    MESSAGE_4,
                    MESSAGE_5,
                    MESSAGE_6,
                    MESSAGE_7,
            ]
        }

        and: "consumer#2 performed seek-to-beginning operation -- no offsets are skipped"
        conditions.eventually {
            consumer02.messages == ALL_MESSAGES
        }

        and: "consumer#3 performed seek to beginning plus 2 -- offsets 0 to 1 are skipped"
        conditions.eventually {
            consumer03.messages == [
                    MESSAGE_2,
                    MESSAGE_3,
                    MESSAGE_4,
                    MESSAGE_5,
                    MESSAGE_6,
                    MESSAGE_7,
            ]
        }

        and: "consumer#4 performed seek-to-end operation -- pre-rebalance messages are skipped"
        conditions.eventually {
            consumer04.messages == MESSAGES_POST_REBALANCE
        }

        and: "consumer#5 performed seek to end minus 2 -- offsets 0 to 2 are skipped"
        conditions.eventually {
            consumer05.messages == [
                    MESSAGE_3,
                    MESSAGE_4,
                    MESSAGE_5,
                    MESSAGE_6,
                    MESSAGE_7,
            ]
        }

        and: "consumer#6 performed seek to today plus 7 days -- falls back to seek-to-end"
        conditions.eventually {
            consumer06.messages == MESSAGES_POST_REBALANCE
        }

        and: "consumer#7 performed seek to epoch -- no offsets are skipped"
        conditions.eventually {
            consumer07.messages == ALL_MESSAGES
        }

        and: "consumer#8 performed seek to current plus 3 -- offsets 0 to 2 are skipped"
        conditions.eventually {
            consumer08.messages == [
                    MESSAGE_3,
                    MESSAGE_4,
                    MESSAGE_5,
                    MESSAGE_6,
                    MESSAGE_7,
            ]
        }

        and: "consumer#9 performed seek to current minus 3 -- no offsets are skipped"
        conditions.eventually {
            consumer09.messages == ALL_MESSAGES
        }

        and: "consumer#10 performed seek to current plus zero -- no offsets are skipped"
        conditions.eventually {
            consumer10.messages == ALL_MESSAGES
        }
    }

    @KafkaClient
    @Requires(property = 'spec.name', value = 'ConsumerSeekAwareSpec')
    static interface MyProducer {
        @Topic(TEST_TOPIC) void produce(String message)
    }

    @Singleton
    @Requires(property = 'spec.name', value = 'ConsumerSeekAwareSpec')
    static class TestMessages {
        TestMessages(MyProducer producer) { MESSAGES_PRE_REBALANCE.forEach(producer::produce) }
    }

    static abstract class MyAbstractConsumer implements ConsumerSeekAware, KafkaSeekOperation.Builder {
        final List<String> messages = []
        @Topic(TEST_TOPIC) void consume(String message) { messages << message }
    }

    @KafkaListener(offsetReset = EARLIEST)
    @Requires(property = 'spec.name', value = 'ConsumerSeekAwareSpec')
    static class MyConsumer01 extends MyAbstractConsumer {
        MyConsumer01(TestMessages test) {}
        boolean revoked = false
        @Override void onPartitionsAssigned(Collection<TopicPartition> partitions, KafkaSeeker seeker) {
            partitions.each(tp -> seeker.perform(seek(tp, 3)))
        }
        @Override void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            revoked = true
        }
        @Override @Topic(patterns = TEST_TOPIC) void consume(String message) {
            messages << message
        }
    }

    @KafkaListener(offsetReset = LATEST)
    @Requires(property = 'spec.name', value = 'ConsumerSeekAwareSpec')
    static class MyConsumer02 extends MyAbstractConsumer {
        MyConsumer02(TestMessages test) {}
        @Override void onPartitionsAssigned(Collection<TopicPartition> partitions, KafkaSeeker seeker) {
            seekToBeginning(partitions).forEach(seeker::perform)
        }
    }

    @KafkaListener(offsetReset = LATEST)
    @Requires(property = 'spec.name', value = 'ConsumerSeekAwareSpec')
    static class MyConsumer03 extends MyAbstractConsumer {
        MyConsumer03(TestMessages test) {}
        @Override void onPartitionsAssigned(Collection<TopicPartition> partitions, KafkaSeeker seeker) {
            partitions.each(tp -> seeker.perform(seekRelativeToBeginning(tp, 2)))
        }
    }

    @KafkaListener(offsetReset = EARLIEST)
    @Requires(property = 'spec.name', value = 'ConsumerSeekAwareSpec')
    static class MyConsumer04 extends MyAbstractConsumer {
        MyConsumer04(TestMessages test) {}
        @Override void onPartitionsAssigned(Collection<TopicPartition> partitions, KafkaSeeker seeker) {
            seekToEnd(partitions).forEach(seeker::perform)
        }
    }

    @KafkaListener(offsetReset = EARLIEST)
    @Requires(property = 'spec.name', value = 'ConsumerSeekAwareSpec')
    static class MyConsumer05 extends MyAbstractConsumer {
        MyConsumer05(TestMessages test) {}
        @Override void onPartitionsAssigned(Collection<TopicPartition> partitions, KafkaSeeker seeker) {
            partitions.each(tp -> seeker.perform(seekRelativeToEnd(tp, 2)))
        }
    }

    @KafkaListener(offsetReset = EARLIEST)
    @Requires(property = 'spec.name', value = 'ConsumerSeekAwareSpec')
    static class MyConsumer06 extends MyAbstractConsumer {
        MyConsumer06(TestMessages test) {}
        @Override void onPartitionsAssigned(Collection<TopicPartition> partitions, KafkaSeeker seeker) {
            seekToTimestamp(partitions, Instant.now().plus(7, DAYS).toEpochMilli()).forEach(seeker::perform)
        }
    }

    @KafkaListener(offsetReset = EARLIEST)
    @Requires(property = 'spec.name', value = 'ConsumerSeekAwareSpec')
    static class MyConsumer07 extends MyAbstractConsumer {
        MyConsumer07(TestMessages test) {}
        @Override void onPartitionsAssigned(Collection<TopicPartition> partitions, KafkaSeeker seeker) {
            seekToTimestamp(partitions, 0L).forEach(seeker::perform)
        }
    }
    @KafkaListener(offsetReset = EARLIEST)
    @Requires(property = 'spec.name', value = 'ConsumerSeekAwareSpec')
    static class MyConsumer08 extends MyAbstractConsumer {
        MyConsumer08(TestMessages test) {}
        @Override void onPartitionsAssigned(Collection<TopicPartition> partitions, KafkaSeeker seeker) {
            partitions.each(tp -> seeker.perform(seekForward(tp, 3)))
        }
    }

    @KafkaListener(offsetReset = EARLIEST)
    @Requires(property = 'spec.name', value = 'ConsumerSeekAwareSpec')
    static class MyConsumer09 extends MyAbstractConsumer {
        MyConsumer09(TestMessages test) {}
        @Override void onPartitionsAssigned(Collection<TopicPartition> partitions, KafkaSeeker seeker) {
            partitions.each(tp -> seeker.perform(seekBackward(tp, 3)))
        }
    }

    @KafkaListener(offsetReset = EARLIEST)
    @Requires(property = 'spec.name', value = 'ConsumerSeekAwareSpec')
    static class MyConsumer10 extends MyAbstractConsumer {
        MyConsumer10(TestMessages test) {}
        @Override void onPartitionsAssigned(Collection<TopicPartition> partitions, KafkaSeeker seeker) {
            partitions.each(tp -> seeker.perform(seekForward(tp, 0)))
        }
    }
}
