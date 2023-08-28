package io.micronaut.kafka.docs.quickstart

import io.micronaut.context.BeanContext
import io.micronaut.context.annotation.Property
import io.micronaut.core.util.StringUtils
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import jakarta.inject.Inject
import org.junit.jupiter.api.Test

@Property(name = "spec.name", value = "QuickstartTest")
@Property(name = "kafka.enabled", value = StringUtils.TRUE)
@MicronautTest
internal class QuickstartTest {

    @Inject
    lateinit var beanContext: BeanContext

    @Test
    fun testSendProduct() {
        // tag::quickstart[]
        val client = beanContext.getBean(ProductClient::class.java)
        client.sendProduct("Nike", "Blue Trainers")
        // end::quickstart[]
    }
}
