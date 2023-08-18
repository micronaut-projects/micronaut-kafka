package io.micronaut.kafka.docs.producer.inject

import io.micronaut.context.ApplicationContext
import org.junit.jupiter.api.Test
import java.util.Map

internal class BookSenderTest {

    // tag::test[]
    @Test
    fun testBookSender() {
        ApplicationContext.run(Map.of<String, Any>( // <1>
            "kafka.enabled", "true", "spec.name", "BookSenderTest")).use { ctx ->
            val bookSender = ctx.getBean(BookSender::class.java) // <2>
            val book = Book("The Stand")
            bookSender.send("Stephen King", book)
        }
    } // end::test[]
}
