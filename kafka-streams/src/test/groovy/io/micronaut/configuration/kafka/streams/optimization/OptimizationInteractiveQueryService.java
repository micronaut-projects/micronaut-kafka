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
package io.micronaut.configuration.kafka.streams.optimization;

import io.micronaut.configuration.kafka.streams.InteractiveQueryService;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import javax.inject.Singleton;
import java.util.Optional;

@Singleton
public class OptimizationInteractiveQueryService {

    private final InteractiveQueryService interactiveQueryService;

    public OptimizationInteractiveQueryService(InteractiveQueryService interactiveQueryService) {
        this.interactiveQueryService = interactiveQueryService;
    }

    /**
     * Method to get the current value of a given key in a KTable.
     *
     * @param stateStore the name of the state store ie "foo-store"
     * @param key        the key to get
     * @return the current value that correlates to the queried key
     */
    public String getValue(String stateStore, String key) {
        Optional<ReadOnlyKeyValueStore<String, String>> queryableStore = interactiveQueryService.getQueryableStore(stateStore, QueryableStoreTypes.keyValueStore());
        return queryableStore.map(kvReadOnlyKeyValueStore -> kvReadOnlyKeyValueStore.get(key)).orElse("");
    }

}
