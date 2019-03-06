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

import io.micronaut.configuration.kafka.config.AbstractKafkaProducerConfiguration;
import io.micronaut.configuration.metrics.annotation.RequiresMetrics;
import io.micronaut.context.annotation.Context;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.event.BeanCreatedEvent;
import io.micronaut.context.event.BeanCreatedEventListener;

import static io.micronaut.configuration.metrics.micrometer.MeterRegistryFactory.MICRONAUT_METRICS_BINDERS;

/**
 * @author graemerocher
 * @since 1.0
 */
@RequiresMetrics
@Requires(property = MICRONAUT_METRICS_BINDERS + ".kafka.enabled", value = "true", defaultValue = "true")
@Context
public class KafkaProducerMetrics extends AbstractKafkaMetrics<AbstractKafkaProducerConfiguration> implements BeanCreatedEventListener<AbstractKafkaProducerConfiguration> {

    @Override
    public AbstractKafkaProducerConfiguration onCreated(BeanCreatedEvent<AbstractKafkaProducerConfiguration> event) {
        return addKafkaMetrics(event);
    }
}

