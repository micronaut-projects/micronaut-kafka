# tag::imports[]
import uuid

from micronaut.configuration.kafka.annotation import KafkaListener, KafkaScope, OffsetReset, Topic
# end::imports[]


# tag::scope[]
@KafkaScope
class ProductMetadata:

    def __init__(self):
        self.correlation_id = str(uuid.uuid4())

    def get_correlation_id(self) -> str:
        return self.correlation_id
# end::scope[]


# tag::listener[]
@KafkaListener(offsetReset=OffsetReset.EARLIEST)
class ProductListener:

    def __init__(self, product_metadata: ProductMetadata):
        self.product_metadata = product_metadata

    @Topic("products")
    def receive(self, product: str) -> None:
        print("Received " + product + " with correlation " + self.product_metadata.get_correlation_id())
# end::listener[]
