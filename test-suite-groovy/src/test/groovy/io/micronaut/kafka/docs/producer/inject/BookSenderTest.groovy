package io.micronaut.kafka.docs.producer.inject

import io.micronaut.context.ApplicationContext
import io.micronaut.testcontainers.kafka.Kafka
import org.apache.kafka.clients.producer.RecordMetadata
import spock.lang.Specification

import java.util.concurrent.Future

class BookSenderTest extends Specification {

    // tag::test[]
    void "test Book Sender"() {
        given:
        def config = new HashMap<>(Kafka.getProperties());
        config.put("kafka.enabled", "true");
        config.put("spec.name", "BookSenderTest");

        ApplicationContext ctx = ApplicationContext.run(config)  // <1>
        BookSender bookSender = ctx.getBean(BookSender) // <2>
        Book book = new Book('The Stand')

        when:
        bookSender.send('Stephen King', book)
        Future<RecordMetadata> stephenKing = bookSender.send('Stephen King', book);
        def recordMetadata = stephenKing.get();

        then:
        noExceptionThrown()
        recordMetadata.topic() == 'books'

        cleanup:
        ctx.close()
    }
    // end::test[]
}
