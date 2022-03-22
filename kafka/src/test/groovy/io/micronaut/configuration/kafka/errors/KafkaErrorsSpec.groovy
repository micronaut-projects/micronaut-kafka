package io.micronaut.configuration.kafka.errors

import groovy.transform.EqualsAndHashCode
import groovy.transform.ToString
import groovy.util.logging.Slf4j
import io.micronaut.configuration.kafka.AbstractEmbeddedServerSpec
import io.micronaut.configuration.kafka.annotation.ErrorStrategy
import io.micronaut.configuration.kafka.annotation.ErrorStrategyValue
import io.micronaut.configuration.kafka.annotation.KafkaClient
import io.micronaut.configuration.kafka.annotation.KafkaKey
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.KafkaPartition
import io.micronaut.configuration.kafka.annotation.OffsetReset
import io.micronaut.configuration.kafka.annotation.OffsetStrategy
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.configuration.kafka.exceptions.KafkaListenerException
import io.micronaut.configuration.kafka.exceptions.KafkaListenerExceptionHandler
import io.micronaut.context.annotation.Requires
import io.micronaut.core.annotation.Introspected
import org.jetbrains.annotations.NotNull
import spock.lang.Shared

import java.util.stream.IntStream

import static io.micronaut.configuration.kafka.annotation.ErrorStrategyValue.RESUME_AT_NEXT_RECORD
import static io.micronaut.configuration.kafka.annotation.ErrorStrategyValue.RETRY_ON_ERROR
import static io.micronaut.configuration.kafka.annotation.OffsetReset.EARLIEST

class KafkaErrorsSpec extends AbstractEmbeddedServerSpec {

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

    protected Map<String, Object> getConfiguration() {
        super.configuration + ['kafka.consumers.default.max.poll.records': 10]
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
