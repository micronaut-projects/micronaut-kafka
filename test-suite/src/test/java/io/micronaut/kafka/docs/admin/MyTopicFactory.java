package io.micronaut.kafka.docs.admin;

// tag::imports[]
import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Requires;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
// end::imports[]

@Requires(property = "spec.name", value = "MyTopicFactoryTest")
// tag::clazz[]
@Requires(bean = AdminClient.class)
@Factory
public class MyTopicFactory {

    @Bean
    CreateTopicsOptions options() {
        return new CreateTopicsOptions().timeoutMs(5000).validateOnly(true).retryOnQuotaViolation(false);
    }

    @Bean
    NewTopic topic1() {
        return new NewTopic("my-new-topic-1", 1, (short) 1);
    }

    @Bean
    NewTopic topic2() {
        return new NewTopic("my-new-topic-2", 2, (short) 1);
    }
}
// end::clazz[]
