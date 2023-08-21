package io.micronaut.kafka.docs.seek.aware

import io.micronaut.context.annotation.Property
import io.micronaut.kafka.docs.Products
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import jakarta.inject.Inject
import spock.lang.Specification
import spock.util.concurrent.PollingConditions

@MicronautTest
@Property(name = "spec.name", value = "ConsumerSeekAwareSpec")
class ConsumerSeekAwareSpec extends Specification {

    @Inject
    ProductListener consumer

    void "test product listener"() {
        expect:
        new PollingConditions(timeout: 5).eventually {
            !consumer.processed.contains(Products.PRODUCT_0)
            consumer.processed.contains(Products.PRODUCT_1)
        }
    }
}
