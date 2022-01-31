package io.micronaut.configuration.kafka.metrics.builder

import spock.lang.Specification

import static io.micronaut.configuration.kafka.metrics.builder.MeterType.FUNCTION_COUNTER
import static io.micronaut.configuration.kafka.metrics.builder.MeterType.GAUGE
import static io.micronaut.configuration.kafka.metrics.builder.MeterType.TIME_GAUGE

class MeterTypeSpec extends Specification {

    void "test all the enums"() {
        expect:
        MeterType.values() == [GAUGE, FUNCTION_COUNTER, TIME_GAUGE]
    }
}
