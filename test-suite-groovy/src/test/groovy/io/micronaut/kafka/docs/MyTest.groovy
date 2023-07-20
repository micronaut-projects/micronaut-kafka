package io.micronaut.kafka.docs

import io.micronaut.configuration.kafka.annotation.*
import io.micronaut.context.annotation.*
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import jakarta.inject.Inject
import spock.lang.Ignore

import static java.util.concurrent.TimeUnit.SECONDS
import static org.awaitility.Awaitility.await

@Property(name = "spec.name", value = "MyTest")
@MicronautTest
@Ignore("It hangs forever in the CI")
class MyTest extends AbstractKafkaTest {

    @Inject
    MyProducer producer
    @Inject
    MyConsumer consumer

    void "test kafka running"() {
        given:
        String message = "hello"

        when:
        producer.produce(message)

        then:
        await().atMost(5, SECONDS).until(() -> consumer.consumed == message)

        cleanup:
        MY_KAFKA.stop()
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
