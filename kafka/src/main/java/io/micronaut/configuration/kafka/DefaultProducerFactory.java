/*
 * Copyright 2017-2022 original authors
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
package io.micronaut.configuration.kafka;

import java.util.Properties;

import io.micronaut.context.annotation.Factory;
import io.micronaut.core.annotation.NonNull;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.Serializer;

/**
 * A default implementation of {@link ProducerFactory} used for creating producer.
 *
 * @author milanspre
 * @since 5.0.0
 */
@Factory
public class DefaultProducerFactory implements ProducerFactory {

    /**
     *
     * Creates kafka producer, could be overridden for further control.
     *
     * @param config properties for producer
     * @param ks key serializer
     * @param vs value serializer
     * @param <K> key type
     * @param <V> value type
     * @since 5.0.0
     * @return new instance of producer
     */
    @Override
    @NonNull
    public <K, V> Producer<K, V> createProducer(Properties config, Serializer<K> ks, Serializer<V> vs) {
        return new KafkaProducer<>(config, ks, vs);
    }
}
