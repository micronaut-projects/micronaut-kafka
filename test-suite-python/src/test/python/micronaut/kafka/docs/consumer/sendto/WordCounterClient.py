from abc import ABC, abstractmethod

from micronaut.configuration.kafka.annotation import KafkaClient, Topic
from micronaut.context.annotation import Requires


@Requires(property="spec.name", value="WordCounterTest")
@KafkaClient("word-counter-producer")
class WordCounterClient(ABC):

    @Topic("tx-incoming-strings")
    @abstractmethod
    def send(self, words: str) -> None:
        ...
