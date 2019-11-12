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

import io.micrometer.core.instrument.MeterRegistry;
import io.micronaut.configuration.kafka.config.AbstractKafkaConsumerConfiguration;
import io.micronaut.configuration.metrics.annotation.RequiresMetrics;
import io.micronaut.context.BeanLocator;
import io.micronaut.context.annotation.Context;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.event.BeanCreatedEvent;
import io.micronaut.context.event.BeanCreatedEventListener;
import javax.annotation.PreDestroy;

import static io.micronaut.configuration.kafka.metrics.AbstractKafkaMetricsReporter.METER_REGISTRIES;
import static io.micronaut.configuration.metrics.micrometer.MeterRegistryFactory.MICRONAUT_METRICS_BINDERS;

/**
 * Binds Kafka Metrics to Micrometer.
 *
 * @author graemerocher
 * @since 1.0
 */
@RequiresMetrics
@Context
@Requires(property = MICRONAUT_METRICS_BINDERS + ".kafka.enabled", value = "true", defaultValue = "true")
public class KafkaConsumerMetrics extends AbstractKafkaMetrics<AbstractKafkaConsumerConfiguration> implements BeanCreatedEventListener<AbstractKafkaConsumerConfiguration>, AutoCloseable {

    private final BeanLocator beanLocator;

    /**
     * Default constructor.
     * @param beanLocator The bean locator
     */
    public KafkaConsumerMetrics(BeanLocator beanLocator) {
        this.beanLocator = beanLocator;
    }

    @Override
    public AbstractKafkaConsumerConfiguration onCreated(BeanCreatedEvent<AbstractKafkaConsumerConfiguration> event) {
        beanLocator.findBean(MeterRegistry.class).ifPresent(METER_REGISTRIES::add);
        return addKafkaMetrics(event, ConsumerKafkaMetricsReporter.class.getName());
    }

    @PreDestroy
    @Override
    public void close() {
        METER_REGISTRIES.clear();
    }
}
