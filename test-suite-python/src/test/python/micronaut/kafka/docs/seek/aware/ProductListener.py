from micronaut.configuration.kafka import ConsumerSeekAware
from micronaut.configuration.kafka.annotation import KafkaListener, Topic
from micronaut.configuration.kafka.seek import KafkaSeeker, KafkaSeekOperation
from micronaut.context.annotation import Requires
from micronaut.kafka.docs.Product import Product
from org.apache.kafka.common import TopicPartition

from .ProductListenerConfiguration import ProductListenerConfiguration


@KafkaListener
@Requires(property="spec.name", value="ConsumerSeekAwareTest")
class ProductListener(ConsumerSeekAware):  # <1>

    def __init__(self, config: ProductListenerConfiguration):
        self.processed: list[Product] = []

    @Topic("wonderful-products")
    def receive(self, product: Product) -> None:
        self.processed.append(product)

    def onPartitionsRevoked(self, partitions: list[TopicPartition]) -> None:  # <2>
        # save offsets here
        pass

    def onPartitionsAssigned(self, partitions: list[TopicPartition], seeker: KafkaSeeker) -> None:  # <3>
        # seek to offset here
        for tp in partitions:
            seeker.perform(KafkaSeekOperation.seek(tp, 1))
