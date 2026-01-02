package io.micronaut.kafka.docs.producer.inject;

import io.micronaut.context.ApplicationContext;
import io.micronaut.testcontainers.kafka.Kafka;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

class BookSenderTest {

    // tag::test[]
    @Test
    void testBookSender() {
        Map<String, String> kafkaProps = Kafka.getProperties();

        Map<String, Object> config = new HashMap<>(kafkaProps);
        config.put("kafka.enabled", "true");
        config.put("spec.name", "BookSenderTest");

        try (ApplicationContext ctx = ApplicationContext.run(config)) {
            BookSender bookSender = ctx.getBean(BookSender.class);
            Book book = new Book("The Stand");
            Future<RecordMetadata> stephenKing = bookSender.send("Stephen King", book);
            assertDoesNotThrow(() -> {
                RecordMetadata recordMetadata = stephenKing.get();
                assertEquals("books", recordMetadata.topic());
            });
        }
    }
    // end::test[]
}
