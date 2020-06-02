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
