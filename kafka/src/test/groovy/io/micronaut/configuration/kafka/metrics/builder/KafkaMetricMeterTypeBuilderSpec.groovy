
package io.micronaut.configuration.kafka.metrics.builder

import io.micrometer.core.instrument.logging.LoggingMeterRegistry
import org.apache.kafka.common.MetricName
import org.apache.kafka.common.metrics.KafkaMetric
import org.apache.kafka.common.metrics.MetricConfig
import org.apache.kafka.common.metrics.stats.Avg
import org.apache.kafka.common.utils.MockTime
import spock.lang.Specification
import spock.lang.Unroll

class KafkaMetricMeterTypeBuilderSpec extends Specification {
    def "building with no values is empty"() {
        expect:
        !KafkaMetricMeterTypeBuilder.newBuilder().build().isPresent()
    }

    @Unroll
    def "can build Meter with varying conditions #name #prefix #isValid"() {
        when:
        def optional = KafkaMetricMeterTypeBuilder.newBuilder()
                .name(name)
                .prefix(prefix)
                .tagFunction(tagFunction)
                .metric(metric)
                .registry(registry)
                .build()

        then:
        optional.isPresent() == isValid
        if (isValid) {
            assert optional.get().id.name == "prefix.name"
        }

        where:
        name   | prefix   | tagFunction         | metric         | registry                   | isValid
        null   | null     | null                | null           | null                       | false
        null   | null     | null                | null           | new LoggingMeterRegistry() | false
        null   | null     | null                | createMetric() | new LoggingMeterRegistry() | false
        null   | null     | createTagFunction() | createMetric() | new LoggingMeterRegistry() | false
        null   | "prefix" | createTagFunction() | createMetric() | new LoggingMeterRegistry() | true
        "name" | "prefix" | createTagFunction() | createMetric() | new LoggingMeterRegistry() | true
    }


    def createMetric() {
        return new KafkaMetric(new Object(),
                new MetricName("name", "group", "description", [:]),
                new Avg(),
                new MetricConfig(),
                new MockTime())
    }

    def createTagFunction() {
        return {}
    }
}
