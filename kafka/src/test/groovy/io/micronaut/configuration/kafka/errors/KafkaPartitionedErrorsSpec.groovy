package io.micronaut.configuration.kafka.errors

import spock.lang.Ignore
import spock.lang.Stepwise
import spock.lang.Retry

import java.util.stream.IntStream

import static io.micronaut.configuration.kafka.annotation.ErrorStrategyValue.RESUME_AT_NEXT_RECORD
import static io.micronaut.configuration.kafka.annotation.ErrorStrategyValue.RETRY_ON_ERROR
import static io.micronaut.configuration.kafka.annotation.OffsetReset.EARLIEST

@Stepwise
@Retry
@Ignore("https://github.com/micronaut-projects/micronaut-kafka/issues/514")
class KafkaPartitionedErrorsSpec extends KafkaErrorsSpec {

    @Override
    void afterKafkaStarted() {
        createTopic("test-topic", 3, 1)
    }

    @Override
    protected Map<String, Object> getConfiguration() {
        super.configuration + [
                'kafka.consumers.default.max.poll.records': 10,
                'spec.name'                               : KafkaErrorsSpec.class.simpleName
        ]
    }

    void "should correctly handle error strategies"() {
        when:
        IntStream.range(0, 30).forEach { i -> producer.send(UUID.randomUUID(), new TestEvent(i)) }

        then:
        conditions.eventually {
            listenerWithErrorStrategyResumeAtNextRecord.exceptions.size() == 1
            listenerWithErrorStrategyResumeAtNextRecord.failed.size() == 1
            listenerWithErrorStrategyResumeAtNextRecord.events.size() == 29

            listenerWithErrorStrategyRetryOnError.exceptions.size() == 1
            listenerWithErrorStrategyRetryOnError.failed.size() == 2 // One retry
            listenerWithErrorStrategyRetryOnError.events.size() == 29

            listenerWithErrorStrategyRetryOnError10Times.exceptions.size() == 1
            listenerWithErrorStrategyRetryOnError10Times.failed.size() == 11 // 10 times retry
            listenerWithErrorStrategyRetryOnError10Times.events.size() == 29

            listenerSyncPerRecordWithErrorStrategyRetryOnError10Times.exceptions.size() == 1
            listenerSyncPerRecordWithErrorStrategyRetryOnError10Times.failed.size() == 11 // 10 times retry
            listenerSyncPerRecordWithErrorStrategyRetryOnError10Times.events.size() == 29

            listenerWithErrorStrategyNone.exceptions.size() == 1
            listenerWithErrorStrategyNone.failed.size() == 1
            listenerWithErrorStrategyNone.events.stream().anyMatch(e -> e.count == 29)
        }
    }

    void "should be correctly processing in partitions"() {
        expect:
        conditions.eventually {
            listenerWithErrorStrategyResumeAtNextRecord.partitions.size() == 3
            listenerWithErrorStrategyRetryOnError.partitions.size() == 3
            listenerWithErrorStrategyRetryOnError10Times.partitions.size() == 3
            listenerSyncPerRecordWithErrorStrategyRetryOnError10Times.partitions.size() == 3
            listenerWithErrorStrategyNone.partitions.size() == 3
        }
    }


    @Shared
    TestListenerWithErrorStrategyResumeAtNextRecord listenerWithErrorStrategyResumeAtNextRecord

    @Shared
    TestListenerWithErrorStrategyRetryOnError listenerWithErrorStrategyRetryOnError

    @Shared
    TestListenerWithErrorStrategyRetryOnError10Times listenerWithErrorStrategyRetryOnError10Times

    @Shared
    TestListenerWithErrorStrategyNone listenerWithErrorStrategyNone

    @Shared
    TestListenerSyncPerRecordWithErrorStrategyRetryOnError10Times listenerSyncPerRecordWithErrorStrategyRetryOnError10Times

    @Shared
    TestProducer producer

    @Override
    protected int getConditionsTimeout() {
        return 120
    }

    void setupSpec() {
        listenerWithErrorStrategyResumeAtNextRecord = context.getBean(TestListenerWithErrorStrategyResumeAtNextRecord)
        listenerWithErrorStrategyRetryOnError = context.getBean(TestListenerWithErrorStrategyRetryOnError)
        listenerWithErrorStrategyRetryOnError10Times = context.getBean(TestListenerWithErrorStrategyRetryOnError10Times)
        listenerWithErrorStrategyNone = context.getBean(TestListenerWithErrorStrategyNone)
        listenerSyncPerRecordWithErrorStrategyRetryOnError10Times = context.getBean(TestListenerSyncPerRecordWithErrorStrategyRetryOnError10Times)
        producer = context.getBean(TestProducer)
    }

    @Introspected
    @EqualsAndHashCode
    static class TestEvent implements Comparable<TestEvent> {

        final int count

        TestEvent(int count) {
            this.count = count
        }

        @Override
        String toString() {
            count
        }

        @Override
        int compareTo(@NotNull TestEvent o) {
            return count <=> o.count
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaErrorsSpec')
    @KafkaListener(offsetReset = EARLIEST, errorStrategy = @ErrorStrategy(value = RESUME_AT_NEXT_RECORD))
    static class TestListenerWithErrorStrategyResumeAtNextRecord extends AbstractTestListener {
    }

    @Requires(property = 'spec.name', value = 'KafkaErrorsSpec')
    @KafkaListener(offsetReset = EARLIEST, errorStrategy = @ErrorStrategy(value = RETRY_ON_ERROR))
    static class TestListenerWithErrorStrategyRetryOnError extends AbstractTestListener {
    }

    @Requires(property = 'spec.name', value = 'KafkaErrorsSpec')
    @KafkaListener(offsetReset = EARLIEST, errorStrategy = @ErrorStrategy(value = RETRY_ON_ERROR, retryCount = 10))
    static class TestListenerWithErrorStrategyRetryOnError10Times extends AbstractTestListener {
    }

    @Requires(property = 'spec.name', value = 'KafkaErrorsSpec')
    @KafkaListener(offsetReset = EARLIEST)
    static class TestListenerWithErrorStrategyNone extends AbstractTestListener {
    }

    @Requires(property = 'spec.name', value = 'KafkaErrorsSpec')
    @KafkaListener(offsetReset = EARLIEST, offsetStrategy = OffsetStrategy.SYNC_PER_RECORD, errorStrategy = @ErrorStrategy(value = RETRY_ON_ERROR, retryCount = 10))
    static class TestListenerSyncPerRecordWithErrorStrategyRetryOnError10Times extends AbstractTestListener {
    }

    @Slf4j
    static abstract class AbstractTestListener implements KafkaListenerExceptionHandler {

        TreeSet<Integer> partitions = []
        List<TestEvent> failed = []
        TreeSet<TestEvent> events = []
        List<KafkaListenerException> exceptions = []

        @Topic("test-topic")
        void receive(@KafkaKey UUID key, @KafkaPartition int partition, TestEvent event) {
            partitions << partition
            if (event.count == 3) {
                failed << event
                throw new IllegalArgumentException("BOOM")
            }
//            System.out.println(partition + " " + event + " " + this)
            events << event
        }

        @Override
        void handle(KafkaListenerException exception) {
            exceptions << exception
        }
    }

    @Requires(property = 'spec.name', value = 'KafkaErrorsSpec')
    @KafkaClient
    static interface TestProducer {

        @Topic("test-topic")
        void send(@KafkaKey UUID key, TestEvent event);
    }
}
