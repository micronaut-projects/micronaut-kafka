import logging
from typing import Annotated

from micronaut.configuration.kafka.annotation import KafkaKey, KafkaListener, Topic
from micronaut.context.annotation import Requires

LOG = logging.getLogger(__name__)


@Requires(property="spec.name", value="TopicsProductListenerTest")
@KafkaListener
class ProductListener:

    # tag::multiTopics[]
    @Topic(["fun-products", "awesome-products"])
    def receive_multi_topics(self, brand: Annotated[str, KafkaKey], name: str) -> None:
        LOG.info("Got Product - %s by %s", name, brand)
    # end::multiTopics[]

    # tag::patternTopics[]
    @Topic(patterns="products-\\w+")
    def receive_pattern_topics(self, brand: Annotated[str, KafkaKey], name: str) -> None:
        LOG.info("Got Product - %s by %s", name, brand)
    # end::patternTopics[]
