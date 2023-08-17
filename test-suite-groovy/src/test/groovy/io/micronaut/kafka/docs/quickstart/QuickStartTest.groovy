package io.micronaut.kafka.docs.quickstart

import io.micronaut.context.ApplicationContext
import io.micronaut.context.annotation.Property
import io.micronaut.kafka.docs.AbstractKafkaTest
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import jakarta.inject.Inject

@Property(name = 'spec.name', value = 'QuickStartTest')
@MicronautTest
class QuickStartTest extends AbstractKafkaTest {

    @Inject
    ApplicationContext applicationContext

    void "test send product"() {
        expect:
        // tag::quickstart[]
        ProductClient client = applicationContext.getBean(ProductClient.class)
        client.sendProduct('Nike', 'Blue Trainers')
        // end::quickstart[]

        cleanup:
        MY_KAFKA.stop()
    }
}
