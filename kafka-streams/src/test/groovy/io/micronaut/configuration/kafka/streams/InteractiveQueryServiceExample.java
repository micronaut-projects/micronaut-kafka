package io.micronaut.configuration.kafka.streams;

import org.apache.kafka.streams.state.QueryableStoreTypes;

import javax.inject.Singleton;

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
     * @param word the key to get, in this case the word as the stream and ktable have been grouped by word
     * @return the Long count of the word in the store
     */
    public Long getWordCount(String stateStore, String word) {
        return interactiveQueryService.getQueryableStore(stateStore, QueryableStoreTypes.<String, Long>keyValueStore()).get(word);
    }

    /**
     * Method to get byte array from a state store using the interactive query service.
     *
     * @param stateStore the name of the state store ie "bar-store"
     * @param blobName the key to get, in this case the name of the blob
     * @return the byte[] stored in the state store
     */
    public byte[] getBytes(String stateStore, String blobName) {
        return interactiveQueryService.getQueryableStore(stateStore, QueryableStoreTypes.<String, byte[]>keyValueStore()).get(blobName);
    }

    /**
     * Method to get value V by key K.
     *
     * @param stateStore the name of the state store ie "baz-store"
     * @param name the key to get
     * @return the value of type V stored in the state store
     */
    public <K,V> V getGenericKeyValue(String stateStore, K name) {
        return interactiveQueryService.getQueryableStore(stateStore, QueryableStoreTypes.<K, V>keyValueStore()).get(name);
    }
}
