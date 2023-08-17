package io.micronaut.kafka.docs.quickstart

import io.micronaut.context.ApplicationContext
import io.micronaut.context.annotation.Property
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import jakarta.inject.Inject
import spock.lang.Specification

@Property(name = 'spec.name', value = 'QuickStartTest')
@MicronautTest
class QuickStartTest extends Specification {

    @Inject
    ApplicationContext applicationContext

    void "test send product"() {
        expect:
        // tag::quickstart[]
        ProductClient client = applicationContext.getBean(ProductClient.class)
        client.sendProduct('Nike', 'Blue Trainers')
        // end::quickstart[]
    }
}
