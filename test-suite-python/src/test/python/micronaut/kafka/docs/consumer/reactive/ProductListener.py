import logging
from typing import Annotated

from micronaut.configuration.kafka.annotation import KafkaKey, KafkaListener, Topic
from micronaut.context.annotation import Requires
from micronaut.core.annotation import Blocking
from micronaut.kafka.docs.Product import Product
from reactor.core.publisher import Mono

LOG = logging.getLogger(__name__)


@Requires(property="spec.name", value="ReactiveProductListenerTest")
@KafkaListener
class ProductListener:

    # tag::method[]
    @Topic("reactive-products")
    def receive(self, brand: Annotated[str, KafkaKey],  # <1>
                product_publisher: Mono[Product]) -> Mono[Product]:  # <2>
        return product_publisher.doOnSuccess(
            lambda product: LOG.info("Got Product - %s by %s", product.name, brand)  # <3>
        )
    # end::method[]

    # tag::blocking[]
    @Blocking
    @Topic("reactive-products")
    def receive_blocking(self, brand: Annotated[str, KafkaKey], product_publisher: Mono[Product]) -> Mono[Product]:
        return product_publisher.doOnSuccess(
            lambda product: LOG.info("Got Product - %s by %s", product.name, brand)
        )
    # end::blocking[]
