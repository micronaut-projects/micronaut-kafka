package io.micronaut.kafka.docs.seek.ops

import io.micronaut.context.annotation.Property
import io.micronaut.kafka.docs.Products
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import jakarta.inject.Inject
import spock.lang.Specification
import spock.util.concurrent.PollingConditions

@MicronautTest
@Property(name = "spec.name", value = "KafkaSeekOperationsSpec")
class KafkaSeekOperationsSpec extends Specification {

    @Inject
    ProductListener consumer

    void "test product listener"() {
        expect:
        new PollingConditions(timeout: 5).eventually {
            consumer.processed.contains(Products.PRODUCT_0)
            !consumer.processed.contains(Products.PRODUCT_1)
        }
    }
}
