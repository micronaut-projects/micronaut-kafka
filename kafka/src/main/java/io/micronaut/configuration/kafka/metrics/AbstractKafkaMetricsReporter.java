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
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;

import javax.annotation.PreDestroy;
import java.io.Closeable;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A {@link MetricsReporter} that binds metrics to micrometer.
 */
@Internal
abstract class AbstractKafkaMetricsReporter implements MetricsReporter, MeterBinder, Closeable {

    static final Collection<MeterRegistry> METER_REGISTRIES = new ConcurrentLinkedQueue<>();

    private List<KafkaMetric> metrics;

    @Override
    public void bindTo(@NonNull MeterRegistry registry) {
        if (!METER_REGISTRIES.contains(registry)) {
            METER_REGISTRIES.add(registry);
        }
    }

    @Override
    public void init(List<KafkaMetric> metrics) {
        this.metrics = metrics;
        for (MeterRegistry meterRegistry : METER_REGISTRIES) {
            for (KafkaMetric metric : metrics) {
                registerMetric(meterRegistry, metric);
            }
        }
    }

    @Override
    public void metricChange(KafkaMetric metric) {
        for (MeterRegistry meterRegistry : METER_REGISTRIES) {
            registerMetric(meterRegistry, metric);
        }
    }

    @Override
    public void metricRemoval(KafkaMetric metric) {
        // no-op (Micrometer doesn't support removal)
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }

    @PreDestroy
    @Override
    public void close() {
        if (metrics != null) {
            metrics.clear();
            metrics = null;
        }
        METER_REGISTRIES.clear();
    }

    private void registerMetric(MeterRegistry meterRegistry, KafkaMetric metric) {
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
                .collect(Collectors.toList());
    }

    /**
     * The tags to include in the gauge. Defaults to just the client-id.
     *
     * @return The tags to include
     */
    protected Set<String> getIncludedTags() {
        HashSet<String> tags = new HashSet<>();
        tags.add("client-id");
        tags.add("topic");
        tags.add("node-id");
        return tags;
    }

    /**
     * Abstract method to implement with the metric prefix for the reporter.
     *
     * @return prefix name
     */
    protected abstract String getMetricPrefix();

}
