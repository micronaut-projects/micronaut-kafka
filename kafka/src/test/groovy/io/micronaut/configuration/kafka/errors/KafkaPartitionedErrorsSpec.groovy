package io.micronaut.configuration.kafka.errors

import spock.lang.Ignore
import spock.lang.Stepwise
import spock.lang.Retry

import java.util.stream.IntStream

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
        return super.getConfiguration() + ['spec.name': KafkaErrorsSpec.class.simpleName]
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

}
