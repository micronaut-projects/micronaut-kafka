/*
 * Copyright 2017-2021 original authors
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
package io.micronaut.configuration.kafka.streams.metrics;

import io.micronaut.configuration.kafka.metrics.AbstractKafkaMetricsReporter;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.common.metrics.KafkaMetric;


/**
 * Kafka streams specific metrics reporter which prefixes all metrics with kafka-streams.
 */
public class KafkaStreamsMetricsReporter extends AbstractKafkaMetricsReporter {

    private static final String STREAMS_GROUP_PREFIX = "stream";

    @Override
    protected String getMetricPrefix() {
        return "kafka-streams";
    }

    @Override
    protected String getMetricName(KafkaMetric metric) {
        String group = metric.metricName().group();
        if (group != null && group.startsWith(STREAMS_GROUP_PREFIX)) {
            return group + "." + metric.metricName().name();
        }
        return super.getMetricName(metric);
    }

    /**
     * Method to close bean.
     */
    @PreDestroy
    @Override
    public void close() {
        super.close();
    }
}
