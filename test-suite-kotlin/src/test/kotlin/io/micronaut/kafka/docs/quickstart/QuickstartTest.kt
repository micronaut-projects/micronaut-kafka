package io.micronaut.kafka.docs.quickstart

import io.micronaut.context.BeanContext
import io.micronaut.context.annotation.Property
import io.micronaut.kafka.docs.AbstractKafkaTest
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import jakarta.inject.Inject
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

@Property(name = "spec.name", value = "QuickstartTest")
@MicronautTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal class QuickstartTest : AbstractKafkaTest() {
    @Inject
    lateinit var beanContext: BeanContext

    @Test
    fun testSendProduct() {
        // tag::quickstart[]
        val client = beanContext.getBean(ProductClient::class.java)
        client.sendProduct("Nike", "Blue Trainers")
        // end::quickstart[]

        MY_KAFKA.stop()
    }
}
