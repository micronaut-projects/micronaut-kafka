package io.micronaut.configuration.kafka.streams.wordcount;

import io.micronaut.configuration.kafka.streams.InteractiveQueryService;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import javax.inject.Singleton;
import java.util.Optional;

/**
 * Example service that uses the InteractiveQueryService in a reusable way.  This is only intended as an example.
 */
@Singleton
public class InteractiveQueryServiceExample {

    private final InteractiveQueryService interactiveQueryService;

    public InteractiveQueryServiceExample(InteractiveQueryService interactiveQueryService) {
        this.interactiveQueryService = interactiveQueryService;
    }

    /**
     * Method to get the word state store and word count from the store using the interactive query service.
     *
     * @param stateStore the name of the state store ie "foo-store"
     * @param word       the key to get, in this case the word as the stream and ktable have been grouped by word
     * @return the Long count of the word in the store
     */
    public Long getWordCount(String stateStore, String word) {
        Optional<ReadOnlyKeyValueStore<String, Long>> queryableStore = interactiveQueryService.getQueryableStore(stateStore, QueryableStoreTypes.keyValueStore());
        return queryableStore.map(kvReadOnlyKeyValueStore -> kvReadOnlyKeyValueStore.get(word)).orElse(0L);
    }

    /**
     * Method to get byte array from a state store using the interactive query service.
     *
     * @param stateStore the name of the state store ie "bar-store"
     * @param blobName   the key to get, in this case the name of the blob
     * @return the byte[] stored in the state store
     */
    public byte[] getBytes(String stateStore, String blobName) {
        Optional<ReadOnlyKeyValueStore<String, byte[]>> queryableStore = interactiveQueryService.getQueryableStore(stateStore, QueryableStoreTypes.keyValueStore());
        return queryableStore.map(stringReadOnlyKeyValueStore -> stringReadOnlyKeyValueStore.get(blobName)).orElse(null);
    }

    /**
     * Method to get value V by key K.
     *
     * @param stateStore the name of the state store ie "baz-store"
     * @param name       the key to get
     * @return the value of type V stored in the state store
     */
    public <K, V> V getGenericKeyValue(String stateStore, K name) {
        Optional<ReadOnlyKeyValueStore<K, V>> queryableStore = interactiveQueryService.getQueryableStore(stateStore, QueryableStoreTypes.<K, V>keyValueStore());
        return queryableStore.map(kvReadOnlyKeyValueStore -> kvReadOnlyKeyValueStore.get(name)).orElse(null);
    }
}
