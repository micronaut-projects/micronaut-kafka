package docs;

// tag::imports[]
import io.micronaut.configuration.kafka.annotation.*;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
// end::imports[]

// tag::clazz[]
@MicronautTest
class MyTest extends AbstractKafkaTest {

    @Inject
    MyProducer producer;
    @Inject
    MyConsumer consumer;

    @Test
    void testKafkaRunning() {
        // Given
        final String message = "hello";
        // When
        producer.produce(message);
        // Then
        await().atMost(5, SECONDS).until(() -> message.equals(consumer.consumed));
    }

    @KafkaClient
    public interface MyProducer {
        @Topic("my-topic")
        void produce(String message);
    }

    @KafkaListener(offsetReset = OffsetReset.EARLIEST)
    public static class MyConsumer {
        String consumed;
        @Topic("my-topic")
        public void consume(String message) {
            consumed = message;
        }
    }
}
// end::imports[]

