package io.micronaut.configuration.kafka.errors

import spock.lang.Stepwise

@Stepwise
class KafkaPartitionedErrorsSpec extends KafkaErrorsSpec {

    @Override
    void afterKafkaStarted() {
        createTopic("partitioned-errors-spec-topic", 3, 1)
    }

    @Override
    protected Map<String, Object> getConfiguration() {
        return super.configuration +
                ['micronaut.executors.default.type': 'FIXED',
                 'micronaut.executors.default.nThreads': 5,
                 'spec.name': KafkaErrorsSpec.class.simpleName,
                 'kafka.consumers.default.max.poll.records': 10,
                  'errors-spec-topic-name': 'partitioned-errors-spec-topic',
                  'kafka.consumers.default.allow.auto.create.topics' : false]
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
