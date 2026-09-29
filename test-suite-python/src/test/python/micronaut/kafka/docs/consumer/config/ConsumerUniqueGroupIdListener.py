from micronaut.configuration.kafka.annotation import KafkaListener
from micronaut.context.annotation import Requires


@Requires(property="spec.name", value="ConfigProductListenerTest")
# tag::annotation[]
@KafkaListener(groupId="myGroup", uniqueGroupId=True)
# end::annotation[]
class ConsumerUniqueGroupIdListener:
    # define topic listeners
    pass
