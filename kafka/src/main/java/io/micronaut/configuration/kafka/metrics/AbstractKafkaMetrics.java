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

import io.micronaut.configuration.kafka.config.AbstractKafkaConfiguration;
import io.micronaut.context.event.BeanCreatedEvent;
import io.micronaut.core.annotation.Internal;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * A class to simplify the producer and consumer metric.reporters default property.
 *
 * @param <T> An abstract kafka configuration
 * @author Christian Oestreich
 * @since 1.0
 */
@Internal
abstract class AbstractKafkaMetrics<T extends AbstractKafkaConfiguration> {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractKafkaMetrics.class);

    /**
     * Method to add a default metric reporter if not otherwise defined.
     *
     * @param event                         The event for bean created of type AbstractKafkaConfiguration
     * @param kafkaMetricsReporterClassName The class name to use for kafka metrics registration
     * @return The bean
     */
    T addKafkaMetrics(BeanCreatedEvent<T> event, String kafkaMetricsReporterClassName) {
        Properties props = event.getBean().getConfig();
        if (!props.containsKey(CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG)) {
            props.put(CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG, kafkaMetricsReporterClassName);
            LOG.debug("Adding kafka property:value of {}:{}", ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG, kafkaMetricsReporterClassName);
        }
        return event.getBean();
    }
}
