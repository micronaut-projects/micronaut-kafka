package io.micronaut.configuration.kafka.metrics

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.micrometer.prometheusmetrics.PrometheusConfig
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import org.apache.kafka.common.MetricName
import org.apache.kafka.common.metrics.KafkaMetric
import org.apache.kafka.common.metrics.MetricConfig
import org.apache.kafka.common.metrics.stats.Avg
import org.apache.kafka.common.metrics.stats.WindowedCount
import org.apache.kafka.common.utils.Time
import spock.lang.AutoCleanup
import spock.lang.Specification

class AbstractKafkaMetricsReporterSpec extends Specification {

    @AutoCleanup
    private final PrometheusMeterRegistry meterRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
            .throwExceptionOnRegistrationFailure()

    void "network metrics keep a stable node-id tag set for prometheus"() {
        given:
        def reporter = new ConsumerKafkaMetricsReporter()
        reporter.bindTo(meterRegistry)

        when:
        reporter.metricChange(createNodeMetric("request-total", [
                (AbstractKafkaMetricsReporter.CLIENT_ID_TAG): "consumer-1"
        ]))
        reporter.metricChange(createNodeMetric("request-total", [
                (AbstractKafkaMetricsReporter.CLIENT_ID_TAG): "consumer-1",
                (AbstractKafkaMetricsReporter.NODE_ID_TAG) : "node--1"
        ]))

        then:
        meterRegistry.meters.size() == 2

        def scrape = meterRegistry.scrape()
        scrape.contains('kafka_consumer_request_total_requests{client_id="consumer-1",node_id=""}')
        scrape.contains('kafka_consumer_request_total_requests{client_id="consumer-1",node_id="node--1"}')
    }

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

    void "metric removal removes meters from all bound registries"() {
        given:
        def firstRegistry = new SimpleMeterRegistry()
        def secondRegistry = new SimpleMeterRegistry()
        def reporter = new TestKafkaMetricsReporter()
        reporter.bindTo(firstRegistry)
        reporter.bindTo(secondRegistry)

        when:
        def metric = createMetric(1)
        reporter.metricChange(metric)
        reporter.metricRemoval(metric)

        then:
        firstRegistry.meters.isEmpty()
        secondRegistry.meters.isEmpty()
    }

    void "close removes registered meters from all bound registries"() {
        given:
        def firstRegistry = new SimpleMeterRegistry()
        def secondRegistry = new SimpleMeterRegistry()
        def reporter = new TestKafkaMetricsReporter()
        reporter.bindTo(firstRegistry)
        reporter.bindTo(secondRegistry)

        when:
        reporter.metricChange(createMetric(1))
        reporter.close()

        then:
        firstRegistry.meters.isEmpty()
        secondRegistry.meters.isEmpty()
    }

    private static KafkaMetric createNodeMetric(String name, Map<String, String> tags) {
        new KafkaMetric(
                new Object(),
                new MetricName(name, "consumer-metrics", "description", tags),
                new WindowedCount(),
                new MetricConfig(),
                Time.SYSTEM
        )
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

