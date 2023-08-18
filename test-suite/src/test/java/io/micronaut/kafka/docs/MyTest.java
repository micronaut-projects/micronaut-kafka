package io.micronaut.kafka.docs;

import io.micronaut.configuration.kafka.annotation.KafkaClient;
import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.OffsetReset;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.micronaut.context.annotation.Property;
import io.micronaut.context.annotation.Requires;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.junit.jupiter.api.Test;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

@Property(name = "spec.name", value = "MyTest")
@MicronautTest
class MyTest {

    @Test
    void testKafkaRunning(MyProducer producer, MyConsumer consumer) {
        final String message = "hello";
        producer.produce(message);
        await().atMost(5, SECONDS).until(() -> message.equals(consumer.consumed));
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
