package io.micronaut.kafka.docs.producer.inject

import io.micronaut.context.ApplicationContext
import io.micronaut.core.util.StringUtils
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

internal class BookSenderTest {

    // tag::test[]
    @Test
    fun testBookSender() {
        ApplicationContext.run(mapOf( // <1>
            "kafka.enabled" to StringUtils.TRUE, "spec.name" to "BookSenderTest")).use { ctx ->
            val bookSender = ctx.getBean(BookSender::class.java) // <2>
            val book = Book("The Stand")
            bookSender.send("Stephen King", book)
            val stephenKing = bookSender.send("Stephen King", book)
            Assertions.assertDoesNotThrow {
                val recordMetadata = stephenKing.get()
                Assertions.assertEquals("books", recordMetadata.topic())
            }
        }
    } // end::test[]
}
