/*
 * Copyright 2017-2020 original authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.micronaut.configuration.kafka.metrics

import io.micrometer.core.instrument.Meter
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.apache.kafka.common.MetricName
import org.apache.kafka.common.metrics.KafkaMetric
import org.apache.kafka.common.metrics.MetricConfig
import org.apache.kafka.common.metrics.stats.Avg
import org.apache.kafka.common.utils.Time
import spock.lang.Specification

/**
 * Unit tests for {@link AbstractKafkaMetricsReporter}.
 *
 * Verifies the two guards that prevent Prometheus tag-key mismatch WARNs:
 *
 * <ol>
 *   <li>Metrics that resolve to <em>no</em> tags (Kafka internal bookkeeping metrics) are
 *       skipped and never registered in the {@link io.micrometer.core.instrument.MeterRegistry}.</li>
 *   <li>{@code node-id} is excluded from the base {@code getIncludedTags()} set so that metrics
 *       registered both with and without a {@code node-id} tag always resolve to the same label
 *       key set ({@code client-id} and/or {@code topic}).</li>
 * </ol>
 *
 * No Kafka broker is required: metrics are constructed directly from
 * {@link org.apache.kafka.common.metrics.KafkaMetric} with hand-crafted
 * {@link org.apache.kafka.common.MetricName} instances.
 */
class AbstractKafkaMetricsReporterSpec extends Specification {

    // Minimal concrete subclass - only getMetricPrefix() needs an implementation.
    private AbstractKafkaMetricsReporter reporter() {
        return new AbstractKafkaMetricsReporter() {
            @Override
            protected String getMetricPrefix() { "kafka.test" }
        }
    }

    private KafkaMetric metric(String name, String group, Map<String, String> tags) {
        return new KafkaMetric(
            new Object(),
            new MetricName(name, group, "test metric", tags),
            new Avg(),
            new MetricConfig(),
            Mock(Time)
        )
    }

    // -----------------------------------------------------------------------
    // Guard 1: empty-tag metrics are skipped
    // -----------------------------------------------------------------------

    def "metric with no tags is not registered in the meter registry"() {
        given:
        def registry = new SimpleMeterRegistry()
        def reporter = reporter()
        reporter.configure(["meter.registry": registry])

        when: "a metric whose MetricName carries no tags is processed"
        def noTagMetric = metric("count", "kafka-metrics-count", [:])
        reporter.init([noTagMetric])

        then: "the registry stays empty - the tagless metric is silently skipped"
        registry.meters.isEmpty()
    }

    def "metricChange with no-tag metric does not register a meter"() {
        given:
        def registry = new SimpleMeterRegistry()
        def reporter = reporter()
        reporter.configure(["meter.registry": registry])
        reporter.init([])

        when:
        reporter.metricChange(metric("count", "kafka-metrics-count", [:]))

        then:
        registry.meters.isEmpty()
    }

    // -----------------------------------------------------------------------
    // Happy path: metric with an included tag IS registered
    // -----------------------------------------------------------------------

    def "metric with client-id tag is registered"() {
        given:
        def registry = new SimpleMeterRegistry()
        def reporter = reporter()
        reporter.configure(["meter.registry": registry])

        when:
        reporter.init([metric("request-rate", "consumer-metrics", ["client-id": "my-app"])])

        then:
        registry.meters.size() > 0
        registry.meters.every { Meter m ->
            m.id.tags.any { it.key == "client_id" || it.key == "client-id" }
        }
    }

    // -----------------------------------------------------------------------
    // Guard 2: node-id is excluded from the base included-tags set
    // -----------------------------------------------------------------------

    def "base getIncludedTags does not contain node-id"() {
        expect:
        reporter().includedTags.contains(AbstractKafkaMetricsReporter.CLIENT_ID_TAG)
        reporter().includedTags.contains(AbstractKafkaMetricsReporter.TOPIC_TAG)
        !reporter().includedTags.contains(AbstractKafkaMetricsReporter.NODE_ID_TAG)
    }

    def "metric with node-id tag is registered without node-id in the meter tags"() {
        given:
        def registry = new SimpleMeterRegistry()
        def reporter = reporter()
        reporter.configure(["meter.registry": registry])

        when: "a metric carrying both client-id and node-id is processed"
        reporter.init([metric("request-total", "consumer-node-metrics",
            ["client-id": "my-app", "node-id": "node--1"])])

        then: "a meter IS registered (client-id is included)"
        registry.meters.size() > 0

        and: "none of the registered meters carry a node-id tag"
        registry.meters.every { Meter m ->
            m.id.tags.every { it.key != "node-id" && it.key != "node_id" }
        }
    }

    def "two metrics differing only by node-id value produce meters with identical tag keys"() {
        given: "two request-total metrics - one with node-id node--1, one with node-id node-1"
        def registry = new SimpleMeterRegistry()
        def reporter = reporter()
        reporter.configure(["meter.registry": registry])

        def m1 = metric("request-total", "consumer-node-metrics",
            ["client-id": "my-app", "node-id": "node--1"])
        def m2 = metric("request-total", "consumer-node-metrics",
            ["client-id": "my-app", "node-id": "node-1"])

        when:
        reporter.init([m1])
        reporter.metricChange(m2)

        then: "both produce meters, and all meters share the same tag key set (no mismatch)"
        def tagKeySets = registry.meters.collect { Meter m ->
            m.id.tags.collect { it.key }.sort() as Set
        }
        tagKeySets.toUnique().size() == 1
    }
}
