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
package io.micronaut.configuration.kafka.metrics.builder;

import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.TimeGauge;
import io.micronaut.core.annotation.Internal;
import io.micronaut.core.util.StringUtils;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;

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

    private MeterRegistry meterRegistry;
    private String name;
    private Function<MetricName, List<Tag>> tagFunction;
    private KafkaMetric kafkaMetric;
    private String prefix;

    /**
     * Construct this here instead of using static map in registry to free memory at runtime.
     */
    private final KafkaMetricMeterTypeRegistry kafkaMetricMeterTypeRegistry = new KafkaMetricMeterTypeRegistry();

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

        if (StringUtils.isEmpty(name)) {
            name = kafkaMetric.metricName().name();
        }

        KafkaMetricMeterType kafkaMetricMeterType = kafkaMetricMeterTypeRegistry.lookup(this.name);

        if (kafkaMetricMeterType.getMeterType() == MeterType.GAUGE && this.kafkaMetric.metricValue() instanceof Double) {
            return Optional.of(Gauge.builder(getMetricName(), () -> (Double) kafkaMetric.metricValue())
                    .tags(tagFunction.apply(kafkaMetric.metricName()))
                    .description(kafkaMetricMeterType.getDescription())
                    .baseUnit(kafkaMetricMeterType.getBaseUnit())
                    .register(meterRegistry));
        } else if (kafkaMetricMeterType.getMeterType() == MeterType.FUNCTION_COUNTER && this.kafkaMetric.metricValue() instanceof Double) {
            return Optional.of(FunctionCounter.builder(getMetricName(), kafkaMetric, value -> (Double) value.metricValue())
                    .tags(tagFunction.apply(kafkaMetric.metricName()))
                    .description(kafkaMetricMeterType.getDescription())
                    .baseUnit(kafkaMetricMeterType.getBaseUnit())
                    .register(meterRegistry));
        } else if (kafkaMetricMeterType.getMeterType() == MeterType.TIME_GAUGE && this.kafkaMetric.metricValue() instanceof Double) {
            return Optional.of(TimeGauge.builder(getMetricName(), kafkaMetric, kafkaMetricMeterType.getTimeUnit(), value -> (Double) value.metricValue())
                    .tags(tagFunction.apply(kafkaMetric.metricName()))
                    .description(kafkaMetricMeterType.getDescription())
                    .register(meterRegistry));
        }

        return Optional.empty();
    }

    private boolean isValid() {
        return tagFunction != null &&
                meterRegistry != null &&
                kafkaMetric != null &&
                StringUtils.isNotEmpty(prefix);
    }

    private String getMetricName() {
        return prefix + "." + name;
    }
}
