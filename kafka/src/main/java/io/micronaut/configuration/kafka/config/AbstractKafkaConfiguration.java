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

import io.micronaut.context.env.Environment;
import io.micronaut.core.util.Toggleable;

import io.micronaut.core.annotation.NonNull;

import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Stream;

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
     * Convert the given map of values to kafka properties.
     * @param environment The env
     * @param values The values
     * @return The kafka properties
     */
    protected static Properties toKafkaProperties(Environment environment, Map<?, ?> values) {
        Properties properties = new Properties();
        values.entrySet().stream().filter(entry -> {
            String key = entry.getKey().toString();
            return Stream.of("embedded", "consumers", "producers", "streams").noneMatch(key::startsWith);
        }).forEach(entry -> {
            Object value = entry.getValue();
            if (environment.canConvert(entry.getValue().getClass(), String.class)) {
                Optional<?> converted = environment.convert(entry.getValue(), String.class);
                if (converted.isPresent()) {
                    value = converted.get();
                }
            }
            properties.setProperty(entry.getKey().toString(), value.toString());
        });
        return properties;
    }

    /**
     * @return The Kafka configuration
     */
    public @NonNull Properties getConfig() {
        if (config != null) {
            return config;
        }
        return new Properties();
    }

    /**
     * Checks if internal properties map contains given key
     * @param key to check
     * @return true if contains given key, false otherwise
     */
    public boolean containsKey(String key) {
        return getConfig().containsKey(key);
    }

    /**
     * Puts all given properties into internal properties map
     * @param properties to put into internal properties map
     */
    public final void putAll(Map<String, String> properties) {
        getConfig().putAll(properties);
    }

}
