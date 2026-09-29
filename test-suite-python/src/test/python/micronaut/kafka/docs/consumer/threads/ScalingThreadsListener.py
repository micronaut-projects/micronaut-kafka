from micronaut.configuration.kafka.annotation import KafkaListener
from micronaut.context.annotation import Requires


@Requires(property="spec.name", value="ScalingThreadsListenerTest")
# tag::annotation[]
@KafkaListener(groupId="myGroup", threads=10)
# end::annotation[]
class ScalingThreadsListener:
    # define API
    pass
