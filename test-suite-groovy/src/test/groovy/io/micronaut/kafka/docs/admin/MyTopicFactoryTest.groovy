package io.micronaut.kafka.docs.admin

import io.micronaut.configuration.kafka.admin.KafkaNewTopics
import io.micronaut.context.annotation.Property
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import jakarta.inject.Inject
import spock.lang.Specification
import spock.util.concurrent.PollingConditions

@MicronautTest
@Property(name = "spec.name", value = "MyTopicFactoryTest")
class MyTopicFactoryTest extends Specification {

    @Inject
    KafkaNewTopics newTopics

    void "test new topics"() {
        expect:
        new PollingConditions(timeout: 5).eventually {
            areNewTopicsDone(newTopics)
            newTopics.getResult().numPartitions("my-new-topic-1").get() == 1
            newTopics.getResult().numPartitions("my-new-topic-2").get() == 2
        }
    }

    // tag::result[]
    boolean areNewTopicsDone(KafkaNewTopics newTopics) {
        newTopics.result.all().done
    }
    // end::result[]
}
