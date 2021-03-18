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
package io.micronaut.configuration.kafka.config;

import io.micronaut.core.util.Toggleable;

import javax.annotation.Nonnull;
import java.util.Properties;

/**
 * An abstract Kafka configuration class.
 *
 * @param <K> The key deserializer type
 * @param <V> The value deserializer type
 * @author Graeme Rocher
 * @since 1.0
 */
public abstract class AbstractKafkaConfiguration<K, V> implements Toggleable {
    /**
     * The default kafka port.
     */
    public static final int DEFAULT_KAFKA_PORT = 9092;
    /**
     * The default prefix used for Kafka configuration.
     */
    public static final String PREFIX = "kafka";

    /**
     * The property to use to enable embedded Kafka.
     */
    public static final String EMBEDDED = "kafka.embedded.enabled";

    /**
     * The topics that should be created.
     */
    public static final String TOPICS = "kafka.topics";

    /**
     * The topics that should be created.
     */
    public static final String EMBEDDED_TOPICS = "kafka.embedded.topics";
    /**
     * The default bootstrap server address.
     */
    public static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:" + DEFAULT_KAFKA_PORT;

    private final Properties config;

    /**
     * Constructs a new instance.
     *
     * @param config The config to use
     */
    protected AbstractKafkaConfiguration(Properties config) {
        this.config = config;
    }

    /**
     * @return The Kafka configuration
     */
    public @Nonnull Properties getConfig() {
        if (config != null) {
            return config;
        }
        return new Properties();
    }

}
