package io.micronaut.configuration.kafka.metrics.builder

import spock.lang.Specification

import static io.micronaut.configuration.kafka.metrics.builder.MeterType.FUNCTION_COUNTER
import static io.micronaut.configuration.kafka.metrics.builder.MeterType.GAUGE
import static io.micronaut.configuration.kafka.metrics.builder.MeterType.TIME_GAUGE

class KafkaMetricMeterTypeRegistrySpec extends Specification {

    void "test lookup method"() {
        given:
        def registry = new KafkaMetricMeterTypeRegistry()

        expect:
        registry.lookup("not exists").meterType == GAUGE
        registry.lookup("").meterType == GAUGE
        registry.lookup(null).meterType == GAUGE
        registry.lookup("records-consumed-total").meterType == FUNCTION_COUNTER
        registry.lookup("io-waittime-total").meterType == TIME_GAUGE
    }
}
