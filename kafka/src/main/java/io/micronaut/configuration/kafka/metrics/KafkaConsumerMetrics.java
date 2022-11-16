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

import io.micrometer.core.instrument.MeterRegistry;
import io.micronaut.configuration.kafka.config.AbstractKafkaConsumerConfiguration;
import io.micronaut.configuration.metrics.annotation.RequiresMetrics;
import io.micronaut.context.BeanLocator;
import io.micronaut.context.annotation.Context;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.event.BeanCreatedEvent;
import io.micronaut.context.event.BeanCreatedEventListener;
import java.util.Optional;
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
public class KafkaConsumerMetrics extends AbstractKafkaMetrics<AbstractKafkaConsumerConfiguration> implements BeanCreatedEventListener<AbstractKafkaConsumerConfiguration> {

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
        Optional<MeterRegistry> optionalMeterRegistry = beanLocator.findBean(MeterRegistry.class);
        if (optionalMeterRegistry.isPresent()) {
            return addKafkaMetrics(event, ConsumerKafkaMetricsReporter.class.getName(), optionalMeterRegistry.get());
        } else {
            return addKafkaMetrics(event, ConsumerKafkaMetricsReporter.class.getName());
        }
    }

}
