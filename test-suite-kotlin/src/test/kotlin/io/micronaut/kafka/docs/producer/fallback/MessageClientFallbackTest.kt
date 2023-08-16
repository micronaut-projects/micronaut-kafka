package io.micronaut.kafka.docs.producer.fallback

import io.micronaut.context.ApplicationContext
import io.micronaut.context.annotation.Property
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import jakarta.inject.Inject
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test

@MicronautTest
@Property(name = "spec.name", value = "MessageClientFallbackSpec")
@Property(name = "kafka.enabled", value = "false")
internal class MessageClientFallbackTest {

    @Inject
    var context: ApplicationContext? = null

    @Test
    fun contextContainsFallbackBean() {
        val bean = context!!.getBean(MessageClientFallback::class.java)

        assertNotNull(bean)
        assertThrows(UnsupportedOperationException::class.java) {
            bean.send("message")
        }
    }
}
