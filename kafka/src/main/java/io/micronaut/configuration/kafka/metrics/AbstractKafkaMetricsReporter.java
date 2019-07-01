/*
 * Copyright 2017-2019 original authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import io.micronaut.core.annotation.Internal;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;

import javax.annotation.PreDestroy;
import java.io.Closeable;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

/**
 * A {@link MetricsReporter} that binds metrics to micrometer.
 */
@Internal
abstract class AbstractKafkaMetricsReporter implements MetricsReporter, MeterBinder, Closeable {

    private static final Collection<MeterRegistry> METER_REGISTRIES = new ConcurrentLinkedQueue<>();

    private List<KafkaMetric> metrics;

    @Override
    public void bindTo(@NonNull MeterRegistry registry) {
        if(!METER_REGISTRIES.contains(registry)) {
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
        MetricName metricName = metric.metricName();
        Object v = metric.metricValue();
        if (v instanceof Double) {
            List<Tag> tags = metricName
                    .tags()
                    .entrySet()
                    .stream()
                    .filter(entry -> getIncludedTags().contains(entry.getKey()))
                    .map(entry -> Tag.of(entry.getKey(), entry.getValue()))
                    .collect(Collectors.toList());
            String name = getMetricPrefix() + '.' + metricName.name();
            meterRegistry.gauge(name, tags, metric, value -> (Double) value.metricValue());
        }
    }


    /**
     * The tags to include in the gauge. Defaults to just the client-id.
     * @return The tags to include
     */
    protected Set<String> getIncludedTags() {
        return Collections.singleton("client-id");
    }

    /**
     * Abstract method to implement with the metric prefix for the reporter.
     *
     * @return prefix name
     */
    protected abstract String getMetricPrefix();


}
