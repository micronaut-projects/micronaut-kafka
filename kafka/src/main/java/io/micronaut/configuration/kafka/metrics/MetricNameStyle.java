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

import io.micronaut.context.exceptions.ConfigurationException;

import java.util.Locale;

/**
 * Supported Kafka client metric naming styles.
 *
 * @author graemerocher
 * @since 5.0
 */
public enum MetricNameStyle {
    /**
     * Use Micrometer compatible metric names such as
     * {@code kafka.consumer.fetch.manager.bytes.consumed.total}.
     */
    MICROMETER,
    /**
     * Preserve the legacy Micronaut metric names such as
     * {@code kafka.consumer.bytes-consumed-total}.
     */
    LEGACY;

    /**
     * @param value The configured value
     * @return The matching metric name style
     */
    public static MetricNameStyle parse(String value) {
        try {
            return valueOf(value.toUpperCase(Locale.ENGLISH));
        } catch (IllegalArgumentException e) {
            throw new ConfigurationException("Invalid Kafka metric name style [" + value + "]. Valid values: micrometer, legacy", e);
        }
    }
}
