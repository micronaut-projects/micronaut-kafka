package io.micronaut.kafka.docs.producer.inject

import io.micronaut.context.ApplicationContext
import spock.lang.Specification

class BookSenderTest extends Specification {

    // tag::test[]
    void "test Book Sender"() {
        expect:
        ApplicationContext ctx = ApplicationContext.run(  // <1>
            "kafka.enabled": true, "spec.name": "BookSenderTest"
        )
        BookSender bookSender = ctx.getBean(BookSender) // <2>
        Book book = new Book('The Stand')
        bookSender.send('Stephen King', book)

        cleanup:
        ctx.close()
    }
    // end::test[]
}
