from micronaut.configuration.kafka.annotation import KafkaListener
from micronaut.context.annotation import Requires


@Requires(property="spec.name", value="ConfigProductListenerTest")
# tag::annotation[]
@KafkaListener("myGroup")
# end::annotation[]
class ConsumerGroupListener:
    # define topic listeners
    pass
