from micronaut.configuration.kafka.annotation import KafkaListener
from micronaut.context.annotation import Requires


@Requires(property="spec.name", value="ConfigProductListenerTest")
# tag::annotation[]
@KafkaListener(groupId="myGroup")
# end::annotation[]
class ConsumerGroupIdListener:
    # define topic listeners
    pass
