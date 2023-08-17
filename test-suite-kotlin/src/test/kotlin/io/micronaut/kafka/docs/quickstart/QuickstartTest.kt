package io.micronaut.kafka.docs.quickstart

import io.micronaut.context.ApplicationContext
import io.micronaut.context.annotation.Property
import io.micronaut.kafka.docs.AbstractKafkaTest
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import jakarta.inject.Inject
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

@Property(name = "spec.name", value = "QuickstartTest")
@MicronautTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal class QuickstartTest {

    @Inject
    var applicationContext: ApplicationContext? = null

    @Test
    fun testSendProduct() {
        // tag::quickstart[]
        val client = applicationContext!!.getBean(ProductClient::class.java)
        client.sendProduct("Nike", "Blue Trainers")
        // end::quickstart[]
    }
}
