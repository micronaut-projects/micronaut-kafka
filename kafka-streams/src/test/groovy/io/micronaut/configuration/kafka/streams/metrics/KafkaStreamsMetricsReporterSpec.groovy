package io.micronaut.configuration.kafka.streams.metrics

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.apache.kafka.common.MetricName
import org.apache.kafka.common.metrics.KafkaMetric
import org.apache.kafka.common.metrics.MetricConfig
import org.apache.kafka.common.metrics.stats.Avg
import org.apache.kafka.common.utils.Time
import spock.lang.Specification

class KafkaStreamsMetricsReporterSpec extends Specification {

    void "stream metric groups are part of the exported metric name"() {
        given:
        def registry = new SimpleMeterRegistry()
        def reporter = new KafkaStreamsMetricsReporter()
        reporter.bindTo(registry)

        when:
        reporter.init([
                createMetric("process-rate", "stream-thread-metrics", ["thread-id": "StreamThread-1"]),
                createMetric("process-rate", "stream-task-metrics", ["task-id": "0_0"]),
                createMetric("fetch-rate", "consumer-fetch-manager-metrics", ["client-id": "stream-consumer"])
        ])

        then:
        registry.find("kafka-streams.stream-thread-metrics.process-rate").meter() != null
        registry.find("kafka-streams.stream-task-metrics.process-rate").meter() != null
        registry.find("kafka-streams.fetch-rate").meter() != null
        registry.find("kafka-streams.process-rate").meter() == null
    }

    private KafkaMetric createMetric(String name, String group, Map<String, String> tags) {
        new KafkaMetric(
                new Object(),
                new MetricName(name, group, "description", tags),
                new Avg(),
                new MetricConfig(),
                Mock(Time)
        )
    }
}
