package io.micronaut.configuration.kafka.metrics.builder

import io.micrometer.core.instrument.Meter
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.micrometer.core.instrument.logging.LoggingMeterRegistry
import org.apache.kafka.common.MetricName
import org.apache.kafka.common.metrics.KafkaMetric
import org.apache.kafka.common.metrics.MetricConfig
import org.apache.kafka.common.metrics.MetricValueProvider
import org.apache.kafka.common.metrics.stats.Avg
import org.apache.kafka.common.utils.Time
import spock.lang.Specification
import spock.lang.Unroll

import java.util.concurrent.atomic.AtomicInteger

class KafkaMetricMeterTypeBuilderSpec extends Specification {

    void "building with no values is empty"() {
        expect:
        !KafkaMetricMeterTypeBuilder.newBuilder().build().isPresent()
    }

    @Unroll
    void "can build Meter with varying conditions #name #prefix #isValid"() {
        when:
        Optional<Meter> optional = KafkaMetricMeterTypeBuilder.newBuilder()
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

    void "re-registering the same meter id replaces the backing kafka metric"() {
        given:
        def registry = new SimpleMeterRegistry()
        def tags = [("client-id"): "consumer-1", topic: "words", partition: "0"]
        def firstValue = new AtomicInteger(2)
        def secondValue = new AtomicInteger(9)

        when:
        KafkaMetricMeterTypeBuilder.newBuilder()
                .prefix("kafka.consumer")
                .tagFunction(createTagFunction())
                .metric(createMetric("records-lag", tags, firstValue))
                .registry(registry)
                .build()

        and:
        def gauge = registry.get("kafka.consumer.records-lag")
                .tags("client-id", "consumer-1", "topic", "words", "partition", "0")
                .gauge()

        then:
        gauge.value() == 2

        when:
        KafkaMetricMeterTypeBuilder.newBuilder()
                .prefix("kafka.consumer")
                .tagFunction(createTagFunction())
                .metric(createMetric("records-lag", tags, secondValue))
                .registry(registry)
                .build()

        then:
        registry.get("kafka.consumer.records-lag")
                .tags("client-id", "consumer-1", "topic", "words", "partition", "0")
                .gauge()
                .value() == 9
    }

    private KafkaMetric createMetric() {
        new KafkaMetric(new Object(),
                new MetricName("name", "group", "description", [:]),
                new Avg(),
                new MetricConfig(),
                Mock(Time))
    }

    private KafkaMetric createMetric(String name, Map<String, String> tags, AtomicInteger value) {
        new KafkaMetric(new Object(),
                new MetricName(name, "group", "description", tags),
                ({ MetricConfig config, long now -> value.get() } as MetricValueProvider<Number>),
                new MetricConfig(),
                Mock(Time))
    }

    private createTagFunction() {
        return { MetricName metricName ->
            metricName.tags().collect { key, value ->
                io.micrometer.core.instrument.Tag.of(key, value)
            }
        }
    }
}
