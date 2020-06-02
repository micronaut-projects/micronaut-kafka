package io.micronaut.configuration.kafka.streams.health

import io.micronaut.configuration.kafka.streams.AbstractTestContainersSpec

class KafkaStreamsHealthDisabledSpec extends AbstractTestContainersSpec {

    def "health check disabled"() {
        when:
        def bean = context.findBean(KafkaStreamsHealth)

        then:
        !bean.isPresent()
    }

    @Override
    protected List<Object> getConfiguration() {
        List<Object> config = super.getConfiguration()
        config.addAll(["kafka.streams.health.enabled", 'false'])
        return config
    }
}
