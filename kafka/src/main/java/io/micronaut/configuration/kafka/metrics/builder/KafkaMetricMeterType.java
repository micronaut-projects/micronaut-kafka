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
package io.micronaut.configuration.kafka.metrics.builder;

import io.micrometer.core.lang.NonNull;
import io.micronaut.core.annotation.Internal;

import java.util.concurrent.TimeUnit;

/**
 * Class for helping to build "typed" metrics.  Will default to GAUGE for all metrics where
 * not registered in {@link KafkaMetricMeterTypeRegistry}.
 *
 * @author Christian Oestreich
 * @since 1.4.1
 */
@Internal
class KafkaMetricMeterType {
    private MeterType meterType = MeterType.GAUGE;
    private String description = null;
    private TimeUnit timeUnit = TimeUnit.MILLISECONDS;
    private String baseUnit = null;

    /**
     * Class for hosing a metric type, description, time unit and base unit.
     */
    KafkaMetricMeterType() {
    }

    /**
     * Class for hosing a metric type, description, time unit and base unit.
     *
     * @param meterType   Meter Type
     * @param description Metric Description
     * @param timeUnit    Time Unit of metric
     * @param baseUnit    Base Unit of metric
     */
    KafkaMetricMeterType(@NonNull final MeterType meterType, final String description, @NonNull final TimeUnit timeUnit, final String baseUnit) {
        this.meterType = meterType;
        this.description = description;
        this.timeUnit = timeUnit;
        this.baseUnit = baseUnit;
    }

    /**
     * Class for hosing a metric type, description, time unit and base unit.
     *
     * @param meterType   Meter Type
     * @param description Metric Description
     * @param baseUnit    Base Unit of metric
     */
    KafkaMetricMeterType(@NonNull final MeterType meterType, final String description, final String baseUnit) {
        this.meterType = meterType;
        this.description = description;
        this.baseUnit = baseUnit;
    }

    /**
     * Class for hosing a metric type, description, time unit and base unit.
     *
     * @param meterType   Meter Type
     * @param description Metric Description
     * @param timeUnit    Time Unit of metric
     */
    KafkaMetricMeterType(@NonNull final MeterType meterType, final String description, @NonNull final TimeUnit timeUnit) {
        this.meterType = meterType;
        this.description = description;
        this.timeUnit = timeUnit;
    }

    /**
     * Class for hosing a metric type, description, time unit and base unit.
     *
     * @param meterType   Meter Type
     * @param description Metric Description
     */
    KafkaMetricMeterType(@NonNull final MeterType meterType, final String description) {
        this.meterType = meterType;
        this.description = description;
    }

    /**
     * Get meter type.
     *
     * @return meter type
     */
    MeterType getMeterType() {
        return meterType;
    }

    /**
     * Get metric description.
     *
     * @return meter description
     */
    String getDescription() {
        return description;
    }

    /**
     * Get metric time unit.
     *
     * @return meter time unit
     */
    TimeUnit getTimeUnit() {
        return timeUnit;
    }

    /**
     * Get metric base unit.
     *
     * @return meter base unit
     */
    String getBaseUnit() {
        return baseUnit;
    }
}
