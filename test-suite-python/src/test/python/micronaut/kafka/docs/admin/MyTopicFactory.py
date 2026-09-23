# tag::imports[]
from micronaut.context.annotation import Bean, Factory, Requires
from org.apache.kafka.clients.admin import AdminClient, CreateTopicsOptions, NewTopic
# end::imports[]


@Requires(property="spec.name", value="MyTopicFactoryTest")
# tag::clazz[]
@Requires(bean=AdminClient)
@Factory
class MyTopicFactory:

    @Bean
    def options(self) -> CreateTopicsOptions:
        return CreateTopicsOptions().timeoutMs(5000).validateOnly(True).retryOnQuotaViolation(False)

    @Bean
    def topic1(self) -> NewTopic:
        return NewTopic("my-new-topic-1", 1, 1)

    @Bean
    def topic2(self) -> NewTopic:
        return NewTopic("my-new-topic-2", 2, 1)
# end::clazz[]
