from micronaut.configuration.kafka.annotation import KafkaListener, OffsetReset, OffsetStrategy, Topic
from micronaut.context.annotation import Requires
from micronaut.kafka.docs.Product import Product
from micronaut.messaging import Acknowledgement


@Requires(property="spec.name", value="AckProductListenerTest")
# tag::clazz[]
@KafkaListener(offsetReset=OffsetReset.EARLIEST, offsetStrategy=OffsetStrategy.DISABLED)  # <1>
class ProductListener:

    @Topic("awesome-products")
    def receive(self, product: Product, acknowledgement: Acknowledgement) -> None:  # <2>
        # process product record
        acknowledgement.ack()  # <3>
# end::clazz[]
