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

import io.micronaut.core.annotation.NonNull;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.core.type.Argument;
import org.apache.kafka.clients.producer.Producer;

/**
 * A registry of managed transactional {@link Producer} instances key by id and type.
 *
 * The producer instance returned will have transactions initialized by executing {@link Producer#initTransactions()}.
 *
 * @author Denis Stepanov
 * @since 4.1.0
 */
public interface TransactionalProducerRegistry {

    /**
     * Returns a transactional managed Producer.
     *
     * @param clientId The client id of the producer.
     * @param transactionalId The transactional id of the producer.
     * @param keyType The key type
     * @param valueType The value type
     * @param <K> The key generic type
     * @param <V> The value generic type
     * @return The producer
     * @since
     */
    @NonNull
    <K, V> Producer<K, V> getTransactionalProducer(@Nullable String clientId, String transactionalId, Argument<K> keyType, Argument<V> valueType);

    /**
     * Closed the producer.
     * Should be used for cases when {@link org.apache.kafka.common.errors.ProducerFencedException} is thrown.
     *
     * @param producer The producer
     */
    void close(@NonNull Producer<?, ?> producer);

}
