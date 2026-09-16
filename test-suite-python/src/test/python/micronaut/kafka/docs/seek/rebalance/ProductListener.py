from micronaut.configuration.kafka import ConsumerAware
from micronaut.configuration.kafka.annotation import KafkaListener, OffsetReset, Topic
from micronaut.context.annotation import Requires
from micronaut.kafka.docs.Product import Product
from org.apache.kafka.clients.consumer import Consumer, ConsumerRebalanceListener
from org.apache.kafka.common import TopicPartition

from .ProductListenerConfiguration import ProductListenerConfiguration


@KafkaListener(offsetReset=OffsetReset.EARLIEST)
@Requires(property="spec.name", value="ConsumerRebalanceListenerTest")
class ProductListener(ConsumerRebalanceListener, ConsumerAware):

    def __init__(self, config: ProductListenerConfiguration):
        self.processed: list[Product] = []
        self.consumer: Consumer | None = None

    def setKafkaConsumer(self, consumer: Consumer) -> None:  # <1>
        self.consumer = consumer

    @Topic("fantastic-products")
    def receive(self, product: Product) -> None:
        self.processed.append(product)

    def onPartitionsRevoked(self, partitions: list[TopicPartition]) -> None:  # <2>
        # save offsets here
        pass

    def onPartitionsAssigned(self, partitions: list[TopicPartition]) -> None:  # <3>
        # seek to offset here
        for partition in partitions:
            self.consumer.seek(partition, 1)
