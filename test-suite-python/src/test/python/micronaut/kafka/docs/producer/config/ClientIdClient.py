from abc import ABC

from micronaut.configuration.kafka.annotation import KafkaClient
from micronaut.context.annotation import Requires


@Requires(property="spec.name", value="ClientIdClientTest")
# tag::annotation[]
@KafkaClient("product-client")
# end::annotation[]
class ClientIdClient(ABC):
    # define client API
    pass
