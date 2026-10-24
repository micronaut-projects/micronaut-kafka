/*
 * Copyright 2017-2026 original authors
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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.serialization.Deserializer;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.util.Properties;

/**
 * Creates the Kafka {@link Consumer} instances used by Micronaut Kafka, including those of
 * {@code @KafkaListener} beans. Replace {@link DefaultConsumerFactory} to use another implementation
 * of the Kafka client API.
 *
 * @since 6.3.0
 */
public interface ConsumerFactory {

    /**
     * Creates a Kafka consumer.
     *
     * @param config properties for the consumer
     * @param kd     key deserializer, or {@code null} to configure it from the properties
     * @param vd     value deserializer, or {@code null} to configure it from the properties
     * @param <K>    key type
     * @param <V>    value type
     * @return new instance of consumer
     */
    @NonNull
    <K, V> Consumer<K, V> createConsumer(Properties config, @Nullable Deserializer<K> kd, @Nullable Deserializer<V> vd);
}
