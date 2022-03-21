package io.micronaut.configuration.kafka.errors

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
}
