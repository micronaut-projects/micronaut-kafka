
package io.micronaut.configuration.kafka.metrics.builder

import spock.lang.Specification

import java.util.concurrent.TimeUnit

class KafkaMetricMeterTypeSpec extends Specification {

    def "should be able to construct with no args"() {
        when:
        KafkaMetricMeterType kafkaMetricMeterType = new KafkaMetricMeterType()

        then:
        kafkaMetricMeterType.meterType == MeterType.GAUGE
        kafkaMetricMeterType.baseUnit == null
        kafkaMetricMeterType.timeUnit == TimeUnit.MILLISECONDS
        kafkaMetricMeterType.description == null
    }

    def "should be able to construct with some args"() {
        when:
        KafkaMetricMeterType kafkaMetricMeterType = new KafkaMetricMeterType(MeterType.FUNCTION_COUNTER, "test")

        then:
        kafkaMetricMeterType.meterType == MeterType.FUNCTION_COUNTER
        kafkaMetricMeterType.baseUnit == null
        kafkaMetricMeterType.timeUnit == TimeUnit.MILLISECONDS
        kafkaMetricMeterType.description == "test"
    }

    def "should be able to construct with more args"() {
        when:
        KafkaMetricMeterType kafkaMetricMeterType = new KafkaMetricMeterType(MeterType.FUNCTION_COUNTER, "test", "test2")

        then:
        kafkaMetricMeterType.meterType == MeterType.FUNCTION_COUNTER
        kafkaMetricMeterType.baseUnit == "test2"
        kafkaMetricMeterType.timeUnit == TimeUnit.MILLISECONDS
        kafkaMetricMeterType.description == "test"
    }

    def "should be able to construct with even more args"() {
        when:
        KafkaMetricMeterType kafkaMetricMeterType = new KafkaMetricMeterType(MeterType.FUNCTION_COUNTER, "test", TimeUnit.SECONDS)

        then:
        kafkaMetricMeterType.meterType == MeterType.FUNCTION_COUNTER
        kafkaMetricMeterType.baseUnit == null
        kafkaMetricMeterType.timeUnit == TimeUnit.SECONDS
        kafkaMetricMeterType.description == "test"
    }

    def "should be able to construct with args"() {
        when:
        KafkaMetricMeterType kafkaMetricMeterType = new KafkaMetricMeterType(MeterType.TIME_GAUGE, "a", TimeUnit.DAYS, "c")

        then:
        kafkaMetricMeterType.meterType == MeterType.TIME_GAUGE
        kafkaMetricMeterType.baseUnit == "c"
        kafkaMetricMeterType.timeUnit == TimeUnit.DAYS
        kafkaMetricMeterType.description == "a"
    }
}
