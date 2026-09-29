from jakarta.inject import Singleton
from micronaut.context.annotation import Requires
from micronaut.kafka.docs.Products import Products

from .ProductClient import ProductClient


@Singleton
@Requires(property="spec.name", value="ConsumerRebalanceListenerTest")
class ProductListenerConfiguration:

    def __init__(self, producer: ProductClient):
        # Records are produced before ProductListener rebalances
        producer.produce(Products.PRODUCT_0)
        producer.produce(Products.PRODUCT_1)
