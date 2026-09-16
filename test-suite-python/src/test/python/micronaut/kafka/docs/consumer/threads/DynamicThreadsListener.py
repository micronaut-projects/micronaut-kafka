from micronaut.configuration.kafka.annotation import KafkaListener
from micronaut.context.annotation import Requires


@Requires(property="spec.name", value="DynamicThreadsListenerTest")
# tag::annotation[]
@KafkaListener(groupId="myGroup", threadsValue="${my.thread.count}")
# end::annotation[]
class DynamicThreadsListener:
    # define API
    pass
