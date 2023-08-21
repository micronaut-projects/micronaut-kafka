package io.micronaut.kafka.docs.producer.inject;

import io.micronaut.context.ApplicationContext;
import org.junit.jupiter.api.Test;

import java.util.Map;

class BookSenderTest {

    // tag::test[]
    @Test
    void testBookSender() {

        try (ApplicationContext ctx = ApplicationContext.run( // <1>
            Map.of("kafka.enabled", "true", "spec.name", "BookSenderTest")
        )) {
            BookSender bookSender = ctx.getBean(BookSender.class); // <2>
            Book book = new Book("The Stand");
            bookSender.send("Stephen King", book);
        }
    }
    // end::test[]
}
