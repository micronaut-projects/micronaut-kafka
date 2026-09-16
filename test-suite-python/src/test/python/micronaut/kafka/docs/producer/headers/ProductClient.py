from abc import ABC

# tag::imports[]
from micronaut.configuration.kafka.annotation import KafkaClient
from micronaut.messaging.annotation import MessageHeader
# end::imports[]


# tag::clazz[]
@KafkaClient(id="product-client")
@MessageHeader(name="X-Token", value="${my.application.token}")
class ProductClient(ABC):
    # define client API
    pass
# end::clazz[]
