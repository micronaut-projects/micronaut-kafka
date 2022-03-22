package io.micronaut.configuration.kafka.errors

import spock.lang.Stepwise

@Stepwise
class KafkaPartitionedErrorsSpec extends KafkaErrorsSpec {

    @Override
    void startKafka() {
        super.startKafka()
        createTopic("test-topic", 3, 1)
    }

    @Override
    protected Map<String, Object> getConfiguration() {
        return super.getConfiguration() + ['spec.name': KafkaErrorsSpec.class.simpleName]
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
