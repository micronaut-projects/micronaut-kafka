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

import io.micronaut.configuration.kafka.config.AbstractKafkaConfiguration;
import io.micronaut.context.event.BeanCreatedEvent;
import io.micronaut.core.annotation.Internal;
import org.apache.kafka.clients.producer.ProducerConfig;

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

    /**
     * Method to add a default metric reporter if not otherwise defined.
     *
     * @param event The event for bean created of type AbstractKafkaConfiguration
     * @return The bean
     */
    T addKafkaMetrics(BeanCreatedEvent<T> event) {
        Properties props = event.getBean().getConfig();
        if (!props.containsKey(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG)) {
            props.put(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG, KafkaMetricsReporter.class.getName());
        }
        return event.getBean();
    }
}
