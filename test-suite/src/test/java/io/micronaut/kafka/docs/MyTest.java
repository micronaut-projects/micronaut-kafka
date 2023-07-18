package io.micronaut.kafka.docs;

import io.micronaut.configuration.kafka.annotation.*;
import io.micronaut.context.annotation.*;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.junit.jupiter.api.*;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

@Property(name = "spec.name", value = "MyTest")
@MicronautTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MyTest extends AbstractKafkaTest {
    @Test
    void testKafkaRunning(MyProducer producer, MyConsumer consumer) {
        final String message = "hello";
        producer.produce(message);
        await().atMost(5, SECONDS).until(() -> message.equals(consumer.consumed));
        MY_KAFKA.stop();
    }

    @Requires(property = "spec.name", value = "MyTest")
    @KafkaClient
    interface MyProducer {
        @Topic("my-topic")
        void produce(String message);
    }

    @Requires(property = "spec.name", value = "MyTest")
    @KafkaListener(offsetReset = OffsetReset.EARLIEST)
    static class MyConsumer {
        String consumed;
        @Topic("my-topic")
        public void consume(String message) {
            consumed = message;
        }
    }
}
