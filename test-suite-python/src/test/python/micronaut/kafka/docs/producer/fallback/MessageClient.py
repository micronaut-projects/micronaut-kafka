from abc import ABC, abstractmethod

from micronaut.configuration.kafka.annotation import KafkaClient, Topic
from micronaut.context.annotation import Requires


@Requires(property="spec.name", value="MessageClientFallbackSpec")
@KafkaClient
class MessageClient(ABC):

    @Topic("messages")
    @abstractmethod
    def send(self, message: str) -> None:
        ...
