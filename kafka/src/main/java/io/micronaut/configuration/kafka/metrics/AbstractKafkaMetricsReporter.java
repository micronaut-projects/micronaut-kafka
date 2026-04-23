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

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.binder.MeterBinder;
import io.micronaut.configuration.kafka.metrics.builder.KafkaMetricMeterTypeBuilder;
import io.micronaut.core.annotation.Internal;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.jspecify.annotations.NonNull;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
    private static final String EMPTY_OPTIONAL_TAG_VALUE = "";
    private static final Set<String> NODE_ID_OPTIONAL_METRICS = Set.of(
            "incoming-byte-rate",
            "incoming-byte-total",
            "outgoing-byte-rate",
            "outgoing-byte-total",
            "request-latency-avg",
            "request-latency-max",
            "request-rate",
            "request-size-avg",
            "request-size-max",
            "request-total",
            "response-rate",
            "response-total"
    );

    private final Collection<MeterRegistry> meterRegistries = new ConcurrentLinkedQueue<>();
    private final Map<MeterRegistry, Map<Meter.Id, Meter>> registeredMeters = new ConcurrentHashMap<>();

    private List<KafkaMetric> metrics;

    @Override
    public void bindTo(@NonNull MeterRegistry registry) {
        if (!meterRegistries.contains(registry)) {
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
        for (MeterRegistry meterRegistry : meterRegistries) {
            removeMetric(meterRegistry, metric);
        }
    }

    @Override
    public void configure(Map<String, ?> configs) {
        Object meterRegistry = configs.get("meter.registry");
        if (meterRegistry != null) {
            meterRegistries.add((MeterRegistry) meterRegistry);
        }
    }

    @PreDestroy
    @Override
    public void close() {
        if (metrics != null) {
            metrics.clear();
            metrics = null;
        }
        registeredMeters.forEach((meterRegistry, meters) -> meters.values().forEach(meterRegistry::remove));
        registeredMeters.clear();
        meterRegistries.clear();
    }

    private void registerMetric(MeterRegistry meterRegistry, KafkaMetric metric) {
        KafkaMetricMeterTypeBuilder.newBuilder()
                .prefix(getMetricPrefix())
                .name(getMetricName(metric))
                .metric(metric)
                .tagFunction(getTagFunction())
                .registry(meterRegistry)
                .build()
                .ifPresent(meter -> registeredMeters
                        .computeIfAbsent(meterRegistry, ignored -> new ConcurrentHashMap<>())
                        .put(meter.getId(), meter));
    }

    private void removeMetric(MeterRegistry meterRegistry, KafkaMetric metric) {
        Map<Meter.Id, Meter> meters = registeredMeters.get(meterRegistry);
        if (meters == null || meters.isEmpty()) {
            return;
        }

        String meterName = getMetricPrefix() + "." + metric.metricName().name();
        Set<Tag> expectedTags = Set.copyOf(getTags(metric.metricName()));
        for (var iterator = meters.entrySet().iterator(); iterator.hasNext(); ) {
            var meterEntry = iterator.next();
            Meter meter = meterEntry.getValue();
            if (meter.getId().getName().equals(meterName) && hasExpectedTags(meter.getId().getTags(), expectedTags)) {
                meterRegistry.remove(meter);
                iterator.remove();
            }
        }
        if (meters.isEmpty()) {
            registeredMeters.remove(meterRegistry, meters);
        }
    }

    private static boolean hasExpectedTags(List<Tag> meterTags, Set<Tag> expectedTags) {
        return meterTags.size() == expectedTags.size() && expectedTags.containsAll(meterTags);
    }

    /**
     * Resolve the exported metric name for the supplied Kafka metric.
     *
     * @param metric The Kafka metric
     * @return The metric name to register with Micrometer
     */
    protected String getMetricName(KafkaMetric metric) {
        return metric.metricName().name();
    }

    private Function<MetricName, List<Tag>> getTagFunction() {
        return this::getTags;
    }

    private List<Tag> getTags(MetricName metricName) {
        Set<String> includedTags = getIncludedTags();
        List<Tag> tags = new ArrayList<>(metricName
                .tags()
                .entrySet()
                .stream()
                .filter(entry -> includedTags.contains(entry.getKey()))
                .map(entry -> Tag.of(entry.getKey(), entry.getValue()))
                .toList());
        if (shouldIncludeEmptyNodeIdTag(metricName, includedTags)) {
            tags.add(Tag.of(NODE_ID_TAG, EMPTY_OPTIONAL_TAG_VALUE));
        }
        return tags;
    }

    private boolean shouldIncludeEmptyNodeIdTag(MetricName metricName, Set<String> includedTags) {
        return includedTags.contains(NODE_ID_TAG)
                && !metricName.tags().containsKey(NODE_ID_TAG)
                && NODE_ID_OPTIONAL_METRICS.contains(metricName.name());
    }

    /**
     * The tags to include in the gauge. Defaults to just the client-id.
     *
     * @return The tags to include
     */
    protected Set<String> getIncludedTags() {
        HashSet<String> tags = new HashSet<>();
        tags.add(CLIENT_ID_TAG);
        tags.add(TOPIC_TAG);
        tags.add(NODE_ID_TAG);
        return tags;
    }

    /**
     * Abstract method to implement with the metric prefix for the reporter.
     *
     * @return prefix name
     */
    protected abstract String getMetricPrefix();

}
