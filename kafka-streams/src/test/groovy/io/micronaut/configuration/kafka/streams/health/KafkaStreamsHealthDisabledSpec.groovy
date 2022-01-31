package io.micronaut.configuration.kafka.streams.health

import io.micronaut.configuration.kafka.streams.AbstractTestContainersSpec

class KafkaStreamsHealthDisabledSpec extends AbstractTestContainersSpec {

    void "health check disabled"() {
        when:
        def bean = context.findBean(KafkaStreamsHealth)

        then:
        !bean.isPresent()
    }

    @Override
    protected List<Object> getConfiguration() {
        List<Object> config = super.configuration
        config << "kafka.health.streams.enabled" << 'false'
        config
    }
}
