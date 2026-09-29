# tag::imports[]
from java.lang import Long
from jakarta.inject import Singleton
from micronaut.configuration.kafka.streams import InteractiveQueryService
from micronaut.context.annotation import Requires
from org.apache.kafka.streams.state import QueryableStoreTypes
# end::imports[]


@Requires(property="spec.name", value="WordCountStreamTest")
# tag::clazz[]
@Singleton
class InteractiveQueryServiceExample:
    """
    Example service that uses the InteractiveQueryService in a reusable way.  This is only intended as an example.
    """

    def __init__(self, interactive_query_service: InteractiveQueryService):
        self.interactive_query_service = interactive_query_service

    def get_word_count(self, state_store: str, word: str) -> Long:
        """
        Method to get the word state store and word count from the store using the interactive query service.

        :param state_store: the name of the state store ie "foo-store"
        :param word: the key to get, in this case the word as the stream and ktable have been grouped by word
        :return: the Long count of the word in the store
        """
        queryable_store = self.interactive_query_service.getQueryableStore(
            state_store, QueryableStoreTypes.keyValueStore())
        return queryable_store.map(lambda kv_read_only_key_value_store: kv_read_only_key_value_store.get(word)).orElse(0)

    def get_bytes(self, state_store: str, blob_name: str) -> bytes:
        """
        Method to get byte array from a state store using the interactive query service.

        :param state_store: the name of the state store ie "bar-store"
        :param blob_name: the key to get, in this case the name of the blob
        :return: the byte[] stored in the state store
        """
        queryable_store = self.interactive_query_service.getQueryableStore(
            state_store, QueryableStoreTypes.keyValueStore())
        return queryable_store.map(lambda string_read_only_key_value_store: string_read_only_key_value_store.get(blob_name)).orElse(None)

    def get_generic_key_value(self, state_store: str, name: object) -> object:
        """
        Method to get value V by key K.

        :param state_store: the name of the state store ie "baz-store"
        :param name: the key to get
        :return: the value of type V stored in the state store
        """
        queryable_store = self.interactive_query_service.getQueryableStore(
            state_store, QueryableStoreTypes.keyValueStore())
        return queryable_store.map(lambda kv_read_only_key_value_store: kv_read_only_key_value_store.get(name)).orElse(None)
# end::clazz[]
