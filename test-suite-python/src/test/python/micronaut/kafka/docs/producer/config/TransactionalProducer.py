from typing import Annotated

from jakarta.inject import Singleton
from micronaut.configuration.kafka.annotation import KafkaClient
from micronaut.context.annotation import Requires
from org.apache.kafka.clients.producer import Producer, ProducerRecord


@Requires(property="spec.name", value="TransactionalProducerTest")
# tag::clazz[]
@Singleton
class TransactionalProducer:

    def __init__(self, producer: Annotated[Producer[str, str], KafkaClient(id="my-client", transactionalId="my-tx-id")]):
        self.producer = producer  # <1>

    def send(self, message: str) -> None:
        try:
            self.producer.beginTransaction()  # <2>
            self.producer.send(ProducerRecord("messages", message))
            self.producer.commitTransaction()  # <3>
        except Exception:
            self.producer.abortTransaction()  # <4>
# end::clazz[]
