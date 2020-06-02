
package io.micronaut.configuration.kafka.metrics.builder

import spock.lang.Specification

class MeterTypeSpec extends Specification {
    def "test all the enums"() {
        expect:
        MeterType.values() == [MeterType.GAUGE, MeterType.FUNCTION_COUNTER, MeterType.TIME_GAUGE]
    }
}
