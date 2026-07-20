package io.micronaut.configuration.kafka.streams

import io.micronaut.configuration.kafka.streams.health.KafkaStreamsHealth
import io.micronaut.context.ApplicationContext
import spock.lang.AutoCleanup

class KafkaStreamsDisabledSpec extends AbstractKafkaSpec {

    @AutoCleanup
    ApplicationContext context = ApplicationContext.run([
            'spec.name'              : 'KafkaStreamsDisabledSpec',
            'kafka.test.initializer.enabled': false,
            'kafka.bootstrap.servers': 'localhost:9092',
            'kafka.streams.enabled'  : false
    ])

    void "global kafka streams disable removes streams beans"() {
        expect:
        !context.findBean(DefaultKafkaStreamsConfiguration).present
        !context.findBean(ConfiguredStreamBuilder).present
        !context.findBean(KafkaStreamsFactory).present
        !context.findBean(KafkaStreamsHealth).present
    }
}
