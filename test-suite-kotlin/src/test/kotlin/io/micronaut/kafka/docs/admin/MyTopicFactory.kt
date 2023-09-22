package io.micronaut.kafka.docs.admin

// tag::imports[]
import io.micronaut.context.annotation.Bean
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Requires
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.CreateTopicsOptions
import org.apache.kafka.clients.admin.NewTopic
// end::imports[]

@Requires(property = "spec.name", value = "MyTopicFactoryTest")
// tag::clazz[]
@Requires(bean = AdminClient::class)
@Factory
class MyTopicFactory {

    @Bean
    fun options(): CreateTopicsOptions {
        return CreateTopicsOptions().timeoutMs(5000).validateOnly(true).retryOnQuotaViolation(false)
    }

    @Bean
    fun topic1(): NewTopic {
        return NewTopic("my-new-topic-1", 1, 1)
    }

    @Bean
    fun topic2(): NewTopic {
        return NewTopic("my-new-topic-2", 2, 1)
    }
}
// end::clazz[]
