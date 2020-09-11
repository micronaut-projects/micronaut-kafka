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
package io.micronaut.configuration.kafka;

import io.micronaut.core.type.Argument;
import org.apache.kafka.clients.producer.KafkaProducer;

import javax.annotation.Nonnull;

/**
 * A registry of managed {@link KafkaProducer} instances key by id and type.
 *
 * @author graemerocher
 * @since 1.0
 * @deprecated Use {@link ProducerRegistry} instead
 */
@Deprecated
public interface KafkaProducerRegistry extends ProducerRegistry {

    /**
     * Returns a managed Producer.
     *
     * @param id The id of the producer.
     * @param keyType The key type
     * @param valueType The value type
     * @param <K> The key generic type
     * @param <V> The value generic type
     * @return The producer
     */
    @Override
    @Nonnull <K, V> KafkaProducer<K, V> getProducer(String id, Argument<K> keyType, Argument<V> valueType);
}
