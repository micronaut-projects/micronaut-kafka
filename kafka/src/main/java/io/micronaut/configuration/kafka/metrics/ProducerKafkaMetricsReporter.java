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
import io.micronaut.core.annotation.Internal;
import io.micronaut.core.annotation.TypeHint;

import javax.annotation.PreDestroy;

/**
 * A {@link org.apache.kafka.common.metrics.MetricsReporter} class for producer metrics.
 */
@Internal
@TypeHint(ProducerKafkaMetricsReporter.class)
public class ProducerKafkaMetricsReporter extends AbstractKafkaMetricsReporter {

    private static final String PRODUCER_PREFIX = AbstractKafkaConfiguration.PREFIX + ".producer";

    /**
     * {@inheritDoc}
     */
    @Override
    protected String getMetricPrefix() {
        return PRODUCER_PREFIX;
    }

    /**
     * Method to close bean.  This must remain here for metrics to continue to function correctly for some reason?!.
     */
    @PreDestroy
    @Override
    public void close() {
        super.close();
    }
}
