package io.micronaut.kafka.docs.producer.inject;

import io.micronaut.context.ApplicationContext;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

class BookSenderTest {

    // tag::test[]
    @Test
    void testBookSender() {

        try (ApplicationContext ctx = ApplicationContext.run( // <1>
            Map.of("kafka.enabled", "true", "spec.name", "BookSenderTest")
        )) {
            BookSender bookSender = ctx.getBean(BookSender.class); // <2>
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
