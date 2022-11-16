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
import io.micronaut.core.annotation.Internal;
import io.micronaut.core.annotation.TypeHint;

import javax.annotation.PreDestroy;
import java.util.HashSet;
import java.util.Set;

/**
 * A {@link org.apache.kafka.common.metrics.MetricsReporter} class for consumer metrics.
 */
@Internal
@TypeHint(ConsumerKafkaMetricsReporter.class)
public class ConsumerKafkaMetricsReporter extends AbstractKafkaMetricsReporter {

    public static final String PARTITION_TAG = "partition";

    private static final String CONSUMER_PREFIX = AbstractKafkaConfiguration.PREFIX + ".consumer";

    /**
     * {@inheritDoc}
     */
    @Override
    protected String getMetricPrefix() {
        return CONSUMER_PREFIX;
    }

    @Override
    protected Set<String> getIncludedTags() {
        HashSet<String> tags = new HashSet<>(super.getIncludedTags());
        tags.add(PARTITION_TAG);
        return tags;
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
