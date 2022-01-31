package io.micronaut.configuration.kafka.metrics.builder

import spock.lang.Specification

import static io.micronaut.configuration.kafka.metrics.builder.MeterType.FUNCTION_COUNTER
import static io.micronaut.configuration.kafka.metrics.builder.MeterType.GAUGE
import static io.micronaut.configuration.kafka.metrics.builder.MeterType.TIME_GAUGE
import static java.util.concurrent.TimeUnit.DAYS
import static java.util.concurrent.TimeUnit.MILLISECONDS
import static java.util.concurrent.TimeUnit.SECONDS

class KafkaMetricMeterTypeSpec extends Specification {

    void "should be able to construct with no args"() {
        when:
        KafkaMetricMeterType kafkaMetricMeterType = new KafkaMetricMeterType()

        then:
        kafkaMetricMeterType.meterType == GAUGE
        kafkaMetricMeterType.baseUnit == null
        kafkaMetricMeterType.timeUnit == MILLISECONDS
        kafkaMetricMeterType.description == null
    }

    void "should be able to construct with some args"() {
        when:
        KafkaMetricMeterType kafkaMetricMeterType = new KafkaMetricMeterType(FUNCTION_COUNTER, "test")

        then:
        kafkaMetricMeterType.meterType == FUNCTION_COUNTER
        kafkaMetricMeterType.baseUnit == null
        kafkaMetricMeterType.timeUnit == MILLISECONDS
        kafkaMetricMeterType.description == "test"
    }

    void "should be able to construct with more args"() {
        when:
        KafkaMetricMeterType kafkaMetricMeterType = new KafkaMetricMeterType(FUNCTION_COUNTER, "test", "test2")

        then:
        kafkaMetricMeterType.meterType == FUNCTION_COUNTER
        kafkaMetricMeterType.baseUnit == "test2"
        kafkaMetricMeterType.timeUnit == MILLISECONDS
        kafkaMetricMeterType.description == "test"
    }

    void "should be able to construct with even more args"() {
        when:
        KafkaMetricMeterType kafkaMetricMeterType = new KafkaMetricMeterType(FUNCTION_COUNTER, "test", SECONDS)

        then:
        kafkaMetricMeterType.meterType == FUNCTION_COUNTER
        kafkaMetricMeterType.baseUnit == null
        kafkaMetricMeterType.timeUnit == SECONDS
        kafkaMetricMeterType.description == "test"
    }

    void "should be able to construct with args"() {
        when:
        KafkaMetricMeterType kafkaMetricMeterType = new KafkaMetricMeterType(TIME_GAUGE, "a", DAYS, "c")

        then:
        kafkaMetricMeterType.meterType == TIME_GAUGE
        kafkaMetricMeterType.baseUnit == "c"
        kafkaMetricMeterType.timeUnit == DAYS
        kafkaMetricMeterType.description == "a"
    }
}
