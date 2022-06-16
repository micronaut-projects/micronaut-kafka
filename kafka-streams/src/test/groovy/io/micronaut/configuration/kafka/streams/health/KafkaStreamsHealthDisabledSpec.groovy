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
    protected Map<String, Object> getConfiguration() {
        super.getConfiguration() + ["kafka.health.streams.enabled": 'false']
    }
}
