package io.micronaut.configuration.kafka.metrics

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.apache.kafka.common.MetricName
import org.apache.kafka.common.metrics.KafkaMetric
import org.apache.kafka.common.metrics.MetricConfig
import org.apache.kafka.common.metrics.stats.Avg
import org.apache.kafka.common.utils.Time
import spock.lang.Specification

class AbstractKafkaMetricsReporterSpec extends Specification {

    void "metric removal removes meters from registry"() {
        given:
        def meterRegistry = new SimpleMeterRegistry()
        def reporter = new TestKafkaMetricsReporter()
        reporter.bindTo(meterRegistry)

        when:
        (0..<10).each { partition ->
            KafkaMetric metric = createMetric(partition)
            reporter.metricChange(metric)
            reporter.metricRemoval(metric)
        }

        then:
        meterRegistry.meters.isEmpty()
    }

    void "close removes registered meters from registry"() {
        given:
        def meterRegistry = new SimpleMeterRegistry()
        def reporter = new TestKafkaMetricsReporter()
        reporter.bindTo(meterRegistry)

        when:
        reporter.metricChange(createMetric(1))
        reporter.close()

        then:
        meterRegistry.meters.isEmpty()
    }

    private static KafkaMetric createMetric(int partition) {
        new KafkaMetric(
                new Object(),
                new MetricName(
                        "records-lag",
                        "consumer-fetch-manager-metrics",
                        "description",
                        [
                                (AbstractKafkaMetricsReporter.CLIENT_ID_TAG): "consumer-1",
                                (AbstractKafkaMetricsReporter.TOPIC_TAG): "topic-${partition}".toString(),
                                (ConsumerKafkaMetricsReporter.PARTITION_TAG): Integer.toString(partition),
                        ]
                ),
                new Avg(),
                new MetricConfig(),
                Time.SYSTEM
        )
    }

    private static final class TestKafkaMetricsReporter extends AbstractKafkaMetricsReporter {

        @Override
        protected String getMetricPrefix() {
            return "kafka.test"
        }

        @Override
        protected Set<String> getIncludedTags() {
            [CLIENT_ID_TAG, TOPIC_TAG, ConsumerKafkaMetricsReporter.PARTITION_TAG] as Set
        }
    }
}
