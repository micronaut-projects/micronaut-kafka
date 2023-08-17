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

import org.apache.kafka.common.serialization.Serializer;

import io.micronaut.core.annotation.Nullable;
import org.apache.kafka.common.utils.Utils;

import java.util.Optional;
import java.util.Properties;

/**
 * Abstract Configuration for Apache Kafka Producer. See https://kafka.apache.org/documentation.html#producerconfigs
 *
 * @param <K> The key deserializer type
 * @param <V> The value deserializer type
 * @author Iván López
 * @author Graeme Rocher
 *
 * @since 1.0
 */
public abstract class AbstractKafkaProducerConfiguration<K, V> extends AbstractKafkaConfiguration<K, V> {

    static final Class DEFAULT_KEY_SERIALIZER = org.apache.kafka.common.serialization.StringSerializer.class;
    static final Class DEFAULT_VALUE_SERIALIZER = org.apache.kafka.common.serialization.StringSerializer.class;

    private Serializer<K> keySerializer;
    private Serializer<V> valueSerializer;

    /**
     * Constructs a new instance.
     *
     * @param config The config to use
     */
    @SuppressWarnings("WeakerAccess")
    protected AbstractKafkaProducerConfiguration(Properties config) {
        super(config);
    }

    /**
     * @return The default key {@link Serializer}
     */
    public Optional<Serializer<K>> getKeySerializer() {
        return Optional.ofNullable(keySerializer);
    }

    /**
     * Sets the key serializer.
     *
     * @param keySerializer The key serializer
     */
    public void setKeySerializer(@Nullable Serializer<K> keySerializer) {
        if (keySerializer != null) {
            keySerializer.configure(Utils.propsToMap(getConfig()), true);
        }
        this.keySerializer = keySerializer;
    }

    /**
     * @return The default value {@link Serializer}
     */
    @SuppressWarnings("WeakerAccess")
    public Optional<Serializer<V>> getValueSerializer() {
        return Optional.ofNullable(valueSerializer);
    }

    /**
     * Sets the default value serializer.
     *
     * @param valueSerializer The value serializer
     */
    public void setValueSerializer(@Nullable Serializer<V> valueSerializer) {
        if (valueSerializer != null) {
            valueSerializer.configure(Utils.propsToMap(getConfig()), false);
        }
        this.valueSerializer = valueSerializer;
    }
}
