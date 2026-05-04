package io.micronaut.kafka.docs;

import io.micronaut.configuration.kafka.annotation.KafkaClient;
import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.OffsetReset;
import io.micronaut.configuration.kafka.annotation.Topic;
import io.micronaut.context.annotation.Property;
import io.micronaut.context.annotation.Requires;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import io.micronaut.test.support.TestPropertyProvider;
import io.micronaut.testcontainers.kafka.Kafka;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Property(name = "spec.name", value = "MyTest")
@MicronautTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MyTest implements TestPropertyProvider {

    @Override
    public Map<String, String> getProperties() {
        return Kafka.getProperties();
    }

    @Test
    void testKafkaRunning(MyProducer producer, MyConsumer consumer) throws InterruptedException {
        final String message = "hello";
        producer.produce(message);
        assertEquals(message, consumer.awaitMessage(15, TimeUnit.SECONDS));
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
        private final LinkedBlockingQueue<String> consumedMessages = new LinkedBlockingQueue<>();

        @Topic("my-topic")
        public void consume(String message) {
            consumedMessages.offer(message);
        }

        String awaitMessage(long timeout, TimeUnit timeUnit) throws InterruptedException {
            return consumedMessages.poll(timeout, timeUnit);
        }
    }
}
