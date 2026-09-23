# tag::imports[]
from java.lang import Long
from java.util import Collections
from micronaut.configuration.kafka.annotation import KafkaListener, OffsetReset, OffsetStrategy, Topic
from micronaut.context.annotation import Requires
from micronaut.kafka.docs.Product import Product
from org.apache.kafka.clients.consumer import Consumer, OffsetAndMetadata
from org.apache.kafka.common import TopicPartition
# end::imports[]


@Requires(property="spec.name", value="ManualProductListenerTest")
# tag::clazz[]
@KafkaListener(offsetReset=OffsetReset.EARLIEST, offsetStrategy=OffsetStrategy.DISABLED)  # <1>
class ProductListener:

    @Topic("awesome-products")
    def receive(self, product: Product, offset: Long, partition: int, topic: str, kafka_consumer: Consumer) -> None:  # <2>
        # process product record
        # commit offsets
        kafka_consumer.commitSync(Collections.singletonMap(  # <3>
            TopicPartition(topic, partition),
            OffsetAndMetadata(offset + 1, "my metadata")
        ))
# end::clazz[]
