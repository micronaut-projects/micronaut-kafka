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

import io.micronaut.context.annotation.ConfigurationProperties;

import static io.micronaut.configuration.metrics.micrometer.MeterRegistryFactory.MICRONAUT_METRICS_BINDERS;

/**
 * Configuration for Kafka client metric binding.
 *
 * @author graemerocher
 * @since 5.0
 */
@ConfigurationProperties(KafkaMetricsConfigurationProperties.PREFIX)
public class KafkaMetricsConfigurationProperties {

    /**
     * Prefix for Kafka metrics binder configuration.
     */
    public static final String PREFIX = MICRONAUT_METRICS_BINDERS + ".kafka";

    private MetricNameStyle metricNameStyle = MetricNameStyle.MICROMETER;

    /**
     * @return The metric naming style. Defaults to {@link MetricNameStyle#MICROMETER}.
     */
    public MetricNameStyle getMetricNameStyle() {
        return metricNameStyle;
    }

    /**
     * @param metricNameStyle The metric naming style
     */
    public void setMetricNameStyle(MetricNameStyle metricNameStyle) {
        if (metricNameStyle != null) {
            this.metricNameStyle = metricNameStyle;
        }
    }
}
