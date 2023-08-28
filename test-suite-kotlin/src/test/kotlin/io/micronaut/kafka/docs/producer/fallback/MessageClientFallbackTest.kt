package io.micronaut.kafka.docs.producer.fallback

import io.micronaut.context.BeanContext
import io.micronaut.context.annotation.Property
import io.micronaut.core.util.StringUtils
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import jakarta.inject.Inject
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test

@MicronautTest
@Property(name = "spec.name", value = "MessageClientFallbackSpec")
@Property(name = "kafka.enabled", value = StringUtils.FALSE)
internal class MessageClientFallbackTest {

    @Inject
    lateinit var context: BeanContext

    @Test
    fun contextContainsFallbackBean() {
        val bean = context.getBean(MessageClientFallback::class.java)

        assertNotNull(bean)
        assertThrows(UnsupportedOperationException::class.java) {
            bean.send("message")
        }
    }
}
