package io.micronaut.configuration.kafka.admin

import io.micronaut.configuration.kafka.AbstractKafkaContainerSpec
import io.micronaut.context.annotation.Property
import io.micronaut.context.annotation.Requires
import io.micronaut.core.annotation.Nullable
import jakarta.inject.Inject
import jakarta.inject.Singleton
import org.apache.kafka.clients.admin.NewTopic

@Property(name = "kafka.health.enabled", value = "false")
class KafkaNoNewTopicsSpec extends AbstractKafkaContainerSpec {

    void "don't create any kafka topics"() {
        expect:
        context.findBean(NewTopic).isEmpty()
        context.findBean(KafkaNewTopics).isEmpty()
        context.getBean(MyService).kafkaNewTopics == null
    }

    @Requires(property = 'spec.name', value = 'KafkaNoNewTopicsSpec')
    @Singleton
    static class MyService {
        @Inject
        @Nullable
        KafkaNewTopics kafkaNewTopics
    }
}
