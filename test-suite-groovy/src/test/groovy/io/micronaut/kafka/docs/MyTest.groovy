package io.micronaut.kafka.docs

import io.micronaut.configuration.kafka.annotation.*
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import jakarta.inject.Inject

import static java.util.concurrent.TimeUnit.SECONDS
import static org.awaitility.Awaitility.await

@MicronautTest
class MyTest extends AbstractKafkaTest {

    @Inject
    MyProducer producer
    @Inject
    MyConsumer consumer

    void "test kafka running"() {
        given:
        def message = "hello"
        when:
        producer.produce(message)
        then:
        await().atMost(5, SECONDS).until(() -> consumer.consumed == message)
        cleanup:
        MY_KAFKA.stop()
    }
}

@KafkaClient
interface MyProducer {
    @Topic("my-topic")
    void produce(String message)
}

@KafkaListener(offsetReset = OffsetReset.EARLIEST)
class MyConsumer {
    String consumed

    @Topic("my-topic")
    void consume(String message) {
        consumed = message
    }
}
