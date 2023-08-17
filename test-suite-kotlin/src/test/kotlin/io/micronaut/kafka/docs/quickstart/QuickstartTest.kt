package io.micronaut.kafka.docs.quickstart

import io.micronaut.context.ApplicationContext
import io.micronaut.context.annotation.Property
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import jakarta.inject.Inject
import org.junit.jupiter.api.Test

@Property(name = "spec.name", value = "QuickstartTest")
@MicronautTest
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
