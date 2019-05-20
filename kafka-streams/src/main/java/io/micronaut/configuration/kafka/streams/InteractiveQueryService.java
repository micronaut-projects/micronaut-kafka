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
package io.micronaut.configuration.kafka.streams;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.QueryableStoreType;

import java.util.Collection;
import java.util.Optional;

/**
 * Services to facilitate the interactive query capabilities of Kafka Streams. This class provides
 * services such as querying for a particular store. This is part of the public API of the kafka streams
 * binder and the users can inject this service in their applications to make use of it.
 *
 * @author Christian Oestreich
 * @since 1.1.1
 */
public class InteractiveQueryService {

    private final Collection<KafkaStreams> streams;

    /**
     * Constructor for interactive query service.
     *
     * @param streams the collection os streams
     */
    InteractiveQueryService(Collection<KafkaStreams> streams) {
        this.streams = streams;
    }

    // tag::getQueryableStore[]

    /**
     * Retrieve and return a queryable store by name created in the application.  If state store is not
     * present an Optional.empty() will be returned.
     *
     * @param storeName name of the queryable store
     * @param storeType type of the queryable store
     * @param <T>       generic queryable store
     * @return queryable store.
     */
    public <T> Optional<T> getQueryableStore(String storeName, QueryableStoreType<T> storeType) {
        for (KafkaStreams kafkaStream : this.streams) {
            try {
                T store = kafkaStream.store(storeName, storeType);
                if (store != null) {
                    return Optional.of(store);
                }
            } catch (InvalidStateStoreException ignored) {
                //pass through
            }
        }
        return Optional.empty();
    }
    // end::getQueryableStore[]
}
