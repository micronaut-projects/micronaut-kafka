from micronaut.configuration.kafka.annotation import KafkaListener, OffsetReset, Topic
from micronaut.configuration.kafka.seek import KafkaSeekOperation, KafkaSeekOperations
from micronaut.context.annotation import Property, Requires
from micronaut.kafka.docs.Product import Product
from org.apache.kafka.common import TopicPartition

from .ProductListenerConfiguration import ProductListenerConfiguration


@KafkaListener(offsetReset=OffsetReset.EARLIEST, properties=Property(name="max.poll.records", value="1"))
@Requires(property="spec.name", value="KafkaSeekOperationsTest")
class ProductListener:

    def __init__(self, config: ProductListenerConfiguration):
        self.processed: list[Product] = []

    @Topic("amazing-products")
    def receive(self, product: Product, ops: KafkaSeekOperations) -> None:  # <1>
        self.processed.append(product)
        ops.defer(KafkaSeekOperation.seekToEnd(TopicPartition("amazing-products", 0)))  # <2>
