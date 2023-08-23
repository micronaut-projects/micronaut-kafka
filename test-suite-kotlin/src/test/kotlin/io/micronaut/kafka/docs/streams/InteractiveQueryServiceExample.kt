package io.micronaut.kafka.docs.streams

// tag::imports[]
import io.micronaut.configuration.kafka.streams.InteractiveQueryService
import io.micronaut.context.annotation.Requires
import jakarta.inject.Singleton
import org.apache.kafka.streams.state.QueryableStoreTypes
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore
// end::imports[]

@Requires(property = "spec.name", value = "WordCountStreamTest")
// tag::clazz[]
/**
 * Example service that uses the InteractiveQueryService in a reusable way.  This is only intended as an example.
 */
@Singleton
class InteractiveQueryServiceExample(private val interactiveQueryService: InteractiveQueryService) {
    /**
     * Method to get the word state store and word count from the store using the interactive query service.
     *
     * @param stateStore the name of the state store ie "foo-store"
     * @param word       the key to get, in this case the word as the stream and ktable have been grouped by word
     * @return the Long count of the word in the store
     */
    fun getWordCount(stateStore: String?, word: String): Long {
        val queryableStore = interactiveQueryService.getQueryableStore(
            stateStore, QueryableStoreTypes.keyValueStore<String, Long>())
        return queryableStore.map { kvReadOnlyKeyValueStore: ReadOnlyKeyValueStore<String, Long> ->
            kvReadOnlyKeyValueStore[word] }.orElse(0L)
    }

    /**
     * Method to get byte array from a state store using the interactive query service.
     *
     * @param stateStore the name of the state store ie "bar-store"
     * @param blobName   the key to get, in this case the name of the blob
     * @return the byte[] stored in the state store
     */
    fun getBytes(stateStore: String?, blobName: String): ByteArray? {
        val queryableStore = interactiveQueryService.getQueryableStore(
            stateStore, QueryableStoreTypes.keyValueStore<String, ByteArray>())
        return queryableStore.map { stringReadOnlyKeyValueStore: ReadOnlyKeyValueStore<String, ByteArray> ->
            stringReadOnlyKeyValueStore[blobName] }.orElse(null)
    }

    /**
     * Method to get value V by key K.
     *
     * @param stateStore the name of the state store ie "baz-store"
     * @param name       the key to get
     * @return the value of type V stored in the state store
     */
    fun <K, V> getGenericKeyValue(stateStore: String?, name: K): V? {
        val queryableStore = interactiveQueryService.getQueryableStore(
            stateStore, QueryableStoreTypes.keyValueStore<K, V>())
        return queryableStore.map { kvReadOnlyKeyValueStore: ReadOnlyKeyValueStore<K, V> ->
            kvReadOnlyKeyValueStore[name] }.orElse(null)
    }
}
// end::clazz[]
