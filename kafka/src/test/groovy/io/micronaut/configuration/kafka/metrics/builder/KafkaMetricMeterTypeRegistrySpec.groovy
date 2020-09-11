
package io.micronaut.configuration.kafka.metrics.builder

import spock.lang.Specification

class KafkaMetricMeterTypeRegistrySpec extends Specification {
    def "test lookup method"() {
        given:
        def registry = new KafkaMetricMeterTypeRegistry()

        expect:
        registry.lookup("not exists").meterType == MeterType.GAUGE
        registry.lookup("").meterType == MeterType.GAUGE
        registry.lookup(null).meterType == MeterType.GAUGE
        registry.lookup("records-consumed-total").meterType == MeterType.FUNCTION_COUNTER
        registry.lookup("io-waittime-total").meterType == MeterType.TIME_GAUGE
    }
}
