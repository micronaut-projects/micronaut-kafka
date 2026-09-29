from abc import ABC

from micronaut.configuration.kafka.annotation import KafkaClient
from micronaut.context.annotation import Prototype, Requires


@Requires(property="spec.name", value="RandomTransactionalIdClientTest")
# tag::clazz[]
@Prototype
@KafkaClient(id="my-client", transactionalId="my-tx-id-${random.uuid}")
class RandomTransactionalIdClient(ABC):
    # define client API
    pass
# end::clazz[]
