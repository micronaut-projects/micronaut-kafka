/*
 * Copyright 2017-2026 original authors
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
package io.micronaut.configuration.kafka.metrics.builder;

import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.TimeGauge;
import io.micronaut.core.annotation.Internal;
import io.micronaut.core.util.StringUtils;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.jspecify.annotations.Nullable;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

/**
 * A builder class for constructing a typed kafka meter.  Will lookup the
 * type in {@link KafkaMetricMeterTypeRegistry}.  Supported meter types can
 * be seen in {@link MeterType}.
 *
 * @author Christian Oestreich
 * @since 1.4.1
 */
@Internal
public class KafkaMetricMeterTypeBuilder {

    private static final KafkaMetricMeterTypeRegistry KAFKA_METRIC_METER_TYPE_REGISTRY = new KafkaMetricMeterTypeRegistry();

    private @Nullable MeterRegistry meterRegistry;
    private @Nullable String name;
    private @Nullable Function<MetricName, List<Tag>> tagFunction;
    private @Nullable KafkaMetric kafkaMetric;
    private @Nullable String prefix;

    /**
     * Method for creating a new builder class.
     *
     * @return builder class
     */
    public static KafkaMetricMeterTypeBuilder newBuilder() {
        return new KafkaMetricMeterTypeBuilder();
    }

    /**
     * Builder method for setting kafka metric.
     *
     * @param kafkaMetric kafka metric class
     * @return builder class
     */
    public KafkaMetricMeterTypeBuilder metric(final KafkaMetric kafkaMetric) {
        this.kafkaMetric = kafkaMetric;
        return this;
    }

    /**
     * Builder method for setting metric prefix.
     *
     * @param prefix Metric prefix
     * @return builder class
     */
    public KafkaMetricMeterTypeBuilder prefix(final String prefix) {
        this.prefix = prefix;
        return this;
    }

    /**
     * Builder method for setting metric name.  This is optional and will b derived form metric if not provided.
     *
     * @param name Metric name
     * @return builder class
     */
    public KafkaMetricMeterTypeBuilder name(final String name) {
        this.name = name;
        return this;
    }

    /**
     * Builder method for setting function to get tags.  This is {@link Function}
     * due to the use of abstract method for getting tags details in AbstractKafkaMetricsReporter.
     *
     * @param tagFunction Function to provide tags
     * @return builder class
     */
    public KafkaMetricMeterTypeBuilder tagFunction(final Function<MetricName, List<Tag>> tagFunction) {
        this.tagFunction = tagFunction;
        return this;
    }

    /**
     * Builder method for setting a {@link MeterRegistry}.
     *
     * @param meterRegistry The meter registry
     * @return builder class
     */
    public KafkaMetricMeterTypeBuilder registry(final MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        return this;
    }

    /**
     * Build and register a typed meter.
     *
     * @return Optional type of {@link Meter}
     */
    public Optional<Meter> build() {
        if (!isValid()) {
            return Optional.empty();
        }

        KafkaMetric currentKafkaMetric = kafkaMetric;
        MeterRegistry currentMeterRegistry = meterRegistry;
        Function<MetricName, List<Tag>> currentTagFunction = tagFunction;
        String currentPrefix = prefix;
        String currentName = name;
        if (currentKafkaMetric == null || currentMeterRegistry == null || currentTagFunction == null || StringUtils.isEmpty(currentPrefix)) {
            return Optional.empty();
        }

        String kafkaMetricName = currentKafkaMetric.metricName().name();
        if (StringUtils.isEmpty(currentName)) {
            currentName = kafkaMetricName;
            name = currentName;
        }

        KafkaMetricMeterType kafkaMetricMeterType = KAFKA_METRIC_METER_TYPE_REGISTRY.lookup(kafkaMetricName);
        List<Tag> tags = currentTagFunction.apply(currentKafkaMetric.metricName());

        if (kafkaMetricMeterType.getMeterType() == MeterType.GAUGE && currentKafkaMetric.metricValue() instanceof Number) {
            removeExistingMeter(tags);
            return Optional.of(Gauge.builder(getMetricName(currentPrefix, currentName), currentKafkaMetric, metric -> ((Number) metric.metricValue()).doubleValue())
                    .tags(tags)
                    .description(kafkaMetricMeterType.getDescription())
                    .baseUnit(kafkaMetricMeterType.getBaseUnit())
                    .register(currentMeterRegistry));
        } else if (kafkaMetricMeterType.getMeterType() == MeterType.FUNCTION_COUNTER && currentKafkaMetric.metricValue() instanceof Number) {
            removeExistingMeter(tags);
            return Optional.of(FunctionCounter.builder(getMetricName(currentPrefix, currentName), currentKafkaMetric, metric -> ((Number) metric.metricValue()).doubleValue())
                    .tags(tags)
                    .description(kafkaMetricMeterType.getDescription())
                    .baseUnit(kafkaMetricMeterType.getBaseUnit())
                    .register(currentMeterRegistry));
        } else if (kafkaMetricMeterType.getMeterType() == MeterType.TIME_GAUGE && currentKafkaMetric.metricValue() instanceof Number) {
            removeExistingMeter(tags);
            return Optional.of(TimeGauge.builder(getMetricName(currentPrefix, currentName), currentKafkaMetric, kafkaMetricMeterType.getTimeUnit(), metric -> ((Number) metric.metricValue()).doubleValue())
                    .tags(tags)
                    .description(kafkaMetricMeterType.getDescription())
                    .register(currentMeterRegistry));
        }

        return Optional.empty();
    }

    private boolean isValid() {
        return tagFunction != null &&
                meterRegistry != null &&
                kafkaMetric != null &&
                StringUtils.isNotEmpty(prefix);
    }

    private String getMetricName(String prefix, String name) {
        return prefix + "." + name;
    }

    private void removeExistingMeter(List<Tag> tags) {
        MeterRegistry currentMeterRegistry = meterRegistry;
        String currentPrefix = prefix;
        String currentName = name;
        if (currentMeterRegistry == null || StringUtils.isEmpty(currentPrefix) || StringUtils.isEmpty(currentName)) {
            return;
        }
        // Look the meter up by its exact id instead of searching the registry: a search copies and scans every meter,
        // which made registering N metrics O(N^2). Meter.Id equality only uses the name and the tags, so the type
        // passed here doesn't matter. removeByPreFilterId does nothing if no such meter is registered.
        // Why GAUGE is fine for every meter this builder registers:
        // - removeByPreFilterId looks the id up in the registry's pre-filter map, a HashMap keyed by Meter.Id, whose
        //   hashCode() and equals() use only the name and the tags.
        // - Gauges and time gauges are registered as Type.GAUGE and function counters as Type.COUNTER; the lookup finds
        //   all of them because the type isn't compared.
        // - The previous find(name).tags(tags).meter() didn't filter by type either.
        Meter.Id id = new Meter.Id(getMetricName(currentPrefix, currentName), Tags.of(tags), null, null, Meter.Type.GAUGE);
        currentMeterRegistry.removeByPreFilterId(id);
    }
}
