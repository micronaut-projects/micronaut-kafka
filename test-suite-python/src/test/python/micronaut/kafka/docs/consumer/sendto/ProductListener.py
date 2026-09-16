import logging

# tag::imports[]
from typing import Annotated

from micronaut.configuration.kafka.annotation import KafkaKey, KafkaListener, OffsetReset, Topic
from micronaut.context.annotation import Requires
from micronaut.kafka.docs.Product import Product
from micronaut.messaging.annotation import SendTo
from org.reactivestreams import Publisher
from reactor.core.publisher import Mono
# end::imports[]

LOG = logging.getLogger(__name__)


@Requires(property="spec.name", value="SendToProductListenerTest")
@KafkaListener(offsetReset=OffsetReset.EARLIEST)
class ProductListener:

    # tag::method[]
    @Topic("sendto-products")  # <1>
    @SendTo("product-quantities")  # <2>
    def receive(self, brand: Annotated[str, KafkaKey], product: Product) -> int:
        LOG.info("Got Product - %s by %s", product.name, brand)
        return product.quantity  # <3>
    # end::method[]

    # tag::reactive[]
    @Topic("sendto-products")  # <1>
    @SendTo("product-quantities")  # <2>
    def receive_product(self, brand: Annotated[str, KafkaKey],
                        product_single: Mono[Product]) -> Publisher[int]:
        def quantity(product: Product) -> int:
            LOG.info("Got Product - %s by %s", product.name, brand)
            return product.quantity  # <3>
        return product_single.map(quantity)
    # end::reactive[]
