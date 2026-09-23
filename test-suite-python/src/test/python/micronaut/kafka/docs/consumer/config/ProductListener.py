import logging

# tag::imports[]
from typing import Annotated

from java.lang import Long
from micronaut.configuration.kafka.annotation import KafkaKey, KafkaListener, Topic
from micronaut.context.annotation import Property, Requires
from micronaut.kafka.docs.Product import Product
from org.apache.kafka.clients.consumer import ConsumerConfig, ConsumerRecord
# end::imports[]

LOG = logging.getLogger(__name__)


@Requires(property="spec.name", value="ConfigProductListenerTest")
# tag::clazz[]
@KafkaListener(
    groupId="products",
    pollTimeout="500ms",
    properties=Property(name=ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, value="10000")
)
class ProductListener:
# end::clazz[]

    # tag::method[]
    @Topic("awesome-products")
    def receive(self,
                brand: Annotated[str, KafkaKey],  # <1>
                product: Product,  # <2>
                offset: Long,  # <3>
                partition: int,  # <4>
                topic: str,  # <5>
                timestamp: Long) -> None:  # <6>
        LOG.info("Got Product - %s by %s", product.name, brand)
    # end::method[]

    # tag::consumeRecord[]
    @Topic("awesome-products")
    def receive_record(self, record: ConsumerRecord[str, Product]) -> None:  # <1>
        product = record.value()  # <2>
        brand = record.key()  # <3>
        LOG.info("Got Product - %s by %s", product.name, brand)
    # end::consumeRecord[]
