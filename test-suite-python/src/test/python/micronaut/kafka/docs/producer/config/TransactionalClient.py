from abc import ABC

from micronaut.configuration.kafka.annotation import KafkaClient
from micronaut.context.annotation import Requires


@Requires(property="spec.name", value="TransactionalClientTest")
# tag::clazz[]
@KafkaClient(id="my-client", transactionalId="my-tx-id")
class TransactionalClient(ABC):
    # define client API
    pass
# end::clazz[]
