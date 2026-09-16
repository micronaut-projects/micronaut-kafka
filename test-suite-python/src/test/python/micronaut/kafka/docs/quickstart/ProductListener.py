import logging
from typing import Annotated

# tag::imports[]
from micronaut.configuration.kafka.annotation import KafkaKey, KafkaListener, OffsetReset, Topic
from micronaut.context.annotation import Requires
# end::imports[]

LOG = logging.getLogger(__name__)


@Requires(property="spec.name", value="QuickstartTest")
# tag::clazz[]
@KafkaListener(offsetReset=OffsetReset.EARLIEST)  # <1>
class ProductListener:

    @Topic("my-products")  # <2>
    def receive(self, brand: Annotated[str, KafkaKey], name: str) -> None:  # <3>
        LOG.info("Got Product - %s by %s", name, brand)
# end::clazz[]
