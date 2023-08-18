package io.micronaut.kafka.docs


import io.micronaut.configuration.kafka.annotation.KafkaClient
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.OffsetReset
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.context.annotation.Property
import io.micronaut.context.annotation.Requires
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import jakarta.inject.Inject
import spock.lang.Specification
import spock.util.concurrent.PollingConditions

@MicronautTest
@Property(name = "spec.name", value = "MyTest")
class MyTest extends Specification {

    @Inject
    MyProducer producer
    @Inject
    MyConsumer consumer

    PollingConditions conditions = new PollingConditions()

    void "test kafka running"() {
        given:
        String message = "hello"

        when:
        producer.produce(message)

        then:
        conditions.within(5) {
            consumer.consumed == message
        }
    }

    @Requires(property = "spec.name", value = "MyTest")
    @KafkaClient
    static interface MyProducer {
        @Topic("my-topic")
        void produce(String message)
    }

    @Requires(property = "spec.name", value = "MyTest")
    @KafkaListener(offsetReset = OffsetReset.EARLIEST)
    static class MyConsumer {
        String consumed

        @Topic("my-topic")
        void consume(String message) {
            consumed = message
        }
    }

}
