from abc import ABC, abstractmethod

# tag::imports[]
from micronaut.configuration.kafka.annotation import KafkaClient, Topic
from micronaut.context.annotation import Requires
# end::imports[]


@Requires(property="spec.name", value="WordCountStreamTest")
# tag::clazz[]
@KafkaClient
class WordCountClient(ABC):

    @Topic("streams-plaintext-input")
    @abstractmethod
    def publish_sentence(self, sentence: str) -> None:
        ...
# end::clazz[]
