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

import io.micronaut.core.annotation.Internal;

import java.util.concurrent.TimeUnit;

/**
 * Class for helping to build "typed" metrics.  Will default to GAUGE for all metrics where
 * not registered in {@link KafkaMetricMeterTypeRegistry}
 */
@Internal
class KafkaMetricMeterType {
    private MeterType meterType = MeterType.GAUGE;
    private String description = null;
    private TimeUnit timeUnit = null;
    private String baseUnit = null;

    KafkaMetricMeterType() {
    }

    KafkaMetricMeterType(final MeterType meterType, final String description, final TimeUnit timeUnit, final String baseUnit) {
        this.meterType = meterType;
        this.description = description;
        this.timeUnit = timeUnit;
        this.baseUnit = baseUnit;
    }

    MeterType getMeterType() {
        return meterType;
    }

    String getDescription() {
        return description;
    }

    TimeUnit getTimeUnit() {
        return timeUnit;
    }

    String getBaseUnit() {
        return baseUnit;
    }
}