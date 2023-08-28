package io.micronaut.kafka.docs.quickstart

import io.micronaut.context.BeanContext
import io.micronaut.context.annotation.Property
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import jakarta.inject.Inject
import spock.lang.Specification

@Property(name = 'spec.name', value = 'QuickStartTest')
@Property(name = 'kafka.enabled', value = 'true')
@MicronautTest
class QuickStartTest extends Specification {

    @Inject
    BeanContext beanContext

    void "test send product"() {
        expect:
        // tag::quickstart[]
        ProductClient client = beanContext.getBean(ProductClient.class)
        client.sendProduct('Nike', 'Blue Trainers')
        // end::quickstart[]
    }
}
