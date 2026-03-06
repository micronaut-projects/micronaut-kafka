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
package io.micronaut.configuration.kafka.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.binder.MeterBinder;
import io.micrometer.core.lang.NonNull;
import io.micronaut.configuration.kafka.metrics.builder.KafkaMetricMeterTypeBuilder;
import io.micronaut.core.annotation.Internal;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;

import java.io.Closeable;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Function;

/**
 * A {@link MetricsReporter} that binds metrics to micrometer.
 */
@Internal
public abstract class AbstractKafkaMetricsReporter implements MetricsReporter, MeterBinder, Closeable {

    public static final String CLIENT_ID_TAG = "client-id";
    public static final String TOPIC_TAG = "topic";
    public static final String NODE_ID_TAG = "node-id";

    private final Collection<MeterRegistry> meterRegistries = new ConcurrentLinkedQueue<>();

    private List<KafkaMetric> metrics;

    @Override
    public void bindTo(@NonNull MeterRegistry registry) {
        if (\!meterRegistries.contains(registry)) {
            meterRegistries.add(registry);
        }
    }

    @Override
    public void init(List<KafkaMetric> metrics) {
        this.metrics = metrics;
        for (MeterRegistry meterRegistry : meterRegistries) {
            for (KafkaMetric metric : metrics) {
                registerMetric(meterRegistry, metric);
            }
        }
    }

    @Override
    public void metricChange(KafkaMetric metric) {
        for (MeterRegistry meterRegistry : meterRegistries) {
            registerMetric(meterRegistry, metric);
        }
    }

    @Override
    public void metricRemoval(KafkaMetric metric) {
        // no-op (Micrometer doesn't support removal)
    }

    @Override
    public void configure(Map<String, ?> configs) {
        Object meterRegistry = configs.get("meter.registry");
        if (meterRegistry \!= null) {
            meterRegistries.add((MeterRegistry) meterRegistry);
        }
    }

    @PreDestroy
    @Override
    public void close() {
        if (metrics \!= null) {
            metrics.clear();
            metrics = null;
        }
        meterRegistries.clear();
    }

    private void registerMetric(MeterRegistry meterRegistry, KafkaMetric metric) {
        List<Tag> tags = getTagFunction().apply(metric.metricName());
        if (tags.isEmpty()) {
            // Skip metrics that resolve to no tags. Kafka's internal bookkeeping metrics
            // (e.g. kafka-metrics-count/count from the root Metrics instance) carry no tags,
            // whereas the same metric name is later registered with tags (e.g. client-id) by
            // the actual client instance. Registering both the tagless and tagged variants with
            // Prometheus causes a WARN because Prometheus requires every occurrence of a given
            // metric name to carry exactly the same set of label (tag) keys.
            return;
        }
        KafkaMetricMeterTypeBuilder.newBuilder()
                .prefix(getMetricPrefix())
                .metric(metric)
                .tagFunction(getTagFunction())
                .registry(meterRegistry)
                .build();
    }

    private Function<MetricName, List<Tag>> getTagFunction() {
        return metricName -> metricName
                .tags()
                .entrySet()
                .stream()
                .filter(entry -> getIncludedTags().contains(entry.getKey()))
                .map(entry -> Tag.of(entry.getKey(), entry.getValue()))
                .toList();
    }

    /**
     * The tags to include in the gauge.
     *
     * <p>Defaults to {@code client-id} and {@code topic}. {@code node-id} is intentionally
     * excluded from the base set: Kafka registers certain metrics (e.g. {@code request-total})
     * both with and without a {@code node-id} tag depending on which internal client instance
     * performs the registration. Including {@code node-id} here causes some occurrences of a
     * metric name to carry {@code [client_id, node_id]} while others carry only
     * {@code [client_id]}, which Prometheus rejects with a tag-key mismatch WARN. Subclasses
     * that genuinely require per-node breakdown may override this method and add
     * {@link #NODE_ID_TAG} explicitly.
     *
     * @return The tags to include
     */
    protected Set<String> getIncludedTags() {
        HashSet<String> tags = new HashSet<>();
        tags.add(CLIENT_ID_TAG);
        tags.add(TOPIC_TAG);
        return tags;
    }

    /**
     * Abstract method to implement with the metric prefix for the reporter.
     *
     * @return prefix name
     */
    protected abstract String getMetricPrefix();

}
