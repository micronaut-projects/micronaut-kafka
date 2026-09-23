import logging
from typing import Annotated

from micronaut.configuration.kafka.annotation import KafkaKey, KafkaListener, OffsetReset, Topic
from micronaut.context.annotation import Requires

LOG = logging.getLogger(__name__)


@Requires(property="spec.name", value="SendToProductListenerTest")
@KafkaListener(offsetReset=OffsetReset.EARLIEST)
class QuantityListener:

    def __init__(self):
        self.quantity: int | None = None

    @Topic("product-quantities")
    def receive(self, brand: Annotated[str, KafkaKey], quantity: int) -> None:
        LOG.info("Got quantity - %s by %s", quantity, brand)
        self.quantity = quantity
