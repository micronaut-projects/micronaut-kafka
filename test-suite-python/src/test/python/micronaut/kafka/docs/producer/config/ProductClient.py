from abc import ABC

# tag::imports[]
from micronaut.configuration.kafka.annotation import KafkaClient
from micronaut.context.annotation import Property, Requires
from org.apache.kafka.clients.producer import ProducerConfig
# end::imports[]


@Requires(property="spec.name", value="ProductClientTest")
# tag::clazz[]
@KafkaClient(
    id="product-client",
    acks=KafkaClient.Acknowledge.ALL,
    properties=Property(name=ProducerConfig.RETRIES_CONFIG, value="5")
)
class ProductClient(ABC):
    # define client API
    pass
# end::clazz[]
