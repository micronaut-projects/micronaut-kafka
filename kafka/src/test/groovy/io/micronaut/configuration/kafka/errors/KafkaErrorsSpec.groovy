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
import io.micronaut.configuration.kafka.annotation.OffsetReset
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.configuration.kafka.exceptions.KafkaListenerException
import io.micronaut.configuration.kafka.exceptions.KafkaListenerExceptionHandler
import io.micronaut.context.annotation.Requires
import io.micronaut.core.annotation.Introspected
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
    TestProducer producer

    @Override
    protected int getConditionsTimeout() {
        return 60
    }

    protected Map<String, Object> getConfiguration() {
        super.configuration + ['kafka.consumers.default.max.poll.records': 10]
    }

    void setupSpec() {
        listenerWithErrorStrategyResumeAtNextRecord = context.getBean(TestListenerWithErrorStrategyResumeAtNextRecord)
        listenerWithErrorStrategyRetryOnError = context.getBean(TestListenerWithErrorStrategyRetryOnError)
        listenerWithErrorStrategyRetryOnError10Times = context.getBean(TestListenerWithErrorStrategyRetryOnError10Times)
        listenerWithErrorStrategyNone = context.getBean(TestListenerWithErrorStrategyNone)
        producer = context.getBean(TestProducer)
    }

    void "should correctly handle error strategies"() {
        when:
        IntStream.range(0, 30).forEach { i -> producer.send(UUID.randomUUID(), new TestEvent(i)) }

        then:
        conditions.eventually {
            listenerWithErrorStrategyResumeAtNextRecord.failed.size() == 1
            listenerWithErrorStrategyResumeAtNextRecord.events.size() == 29
            listenerWithErrorStrategyResumeAtNextRecord.exceptions.size() == 1

            listenerWithErrorStrategyRetryOnError.failed.size() == 2 // One retry
            listenerWithErrorStrategyRetryOnError.events.size() == 29
            listenerWithErrorStrategyRetryOnError.exceptions.size() == 1

            listenerWithErrorStrategyRetryOnError10Times.failed.size() == 11 // 10 times retry
            listenerWithErrorStrategyRetryOnError10Times.events.size() == 29
            listenerWithErrorStrategyRetryOnError10Times.exceptions.size() == 1

            listenerWithErrorStrategyNone.failed.size() == 1
            listenerWithErrorStrategyNone.events.stream().anyMatch(e -> e.count == 29)
            listenerWithErrorStrategyNone.exceptions.size() == 1
        }
    }

    @Introspected
    @EqualsAndHashCode
    @ToString
    static class TestEvent {

        int count

        TestEvent() {
        }

        TestEvent(int count) {
            this.count = count
        }
    }

    @KafkaListener(offsetReset = EARLIEST, errorStrategy = @ErrorStrategy(value = RESUME_AT_NEXT_RECORD))
    static class TestListenerWithErrorStrategyResumeAtNextRecord extends AbstractTestListener {
    }

    @KafkaListener(offsetReset = EARLIEST, errorStrategy = @ErrorStrategy(value = RETRY_ON_ERROR))
    static class TestListenerWithErrorStrategyRetryOnError extends AbstractTestListener {
    }

    @KafkaListener(offsetReset = EARLIEST, errorStrategy = @ErrorStrategy(value = RETRY_ON_ERROR, retryCount = 10))
    static class TestListenerWithErrorStrategyRetryOnError10Times extends AbstractTestListener {
    }

    @KafkaListener(offsetReset = EARLIEST)
    static class TestListenerWithErrorStrategyNone extends AbstractTestListener {
    }

    @Slf4j
    @Requires(property = 'spec.name', value = 'KafkaErrorsSpec')
    static abstract class AbstractTestListener implements KafkaListenerExceptionHandler {

        List<TestEvent> failed = []
        Set<TestEvent> events = []
        List<KafkaListenerException> exceptions = []

        @Topic("test-topic")
        void receive(@KafkaKey UUID key, TestEvent event) {
            if (event.count == 3) {
                failed << event
                throw new IllegalArgumentException("BOOM")
            }
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
