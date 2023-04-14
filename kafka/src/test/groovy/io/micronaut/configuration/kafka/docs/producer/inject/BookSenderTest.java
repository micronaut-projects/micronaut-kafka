package io.micronaut.configuration.kafka.docs.producer.inject;

import io.micronaut.configuration.kafka.docs.consumer.batch.Book;
import io.micronaut.context.ApplicationContext;
import org.junit.Test;

public class BookSenderTest {

    // tag::test[]
    @Test
    public void testBookSender() {
        try (ApplicationContext ctx = ApplicationContext.run()) { // <1>
            BookSender bookSender = ctx.getBean(BookSender.class); // <2>
            Book book = new Book();
            book.setTitle("The Stand");
            bookSender.send("Stephen King", book);
        }
    }
    // end::test[]
}
