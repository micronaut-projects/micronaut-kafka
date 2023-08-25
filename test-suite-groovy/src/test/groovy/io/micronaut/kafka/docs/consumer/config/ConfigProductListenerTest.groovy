package io.micronaut.kafka.docs.consumer.config

import io.micronaut.context.ApplicationContext
import io.micronaut.kafka.docs.Product
import spock.lang.Specification

class ConfigProductListenerTest extends Specification {

    void "test Send Product"() {
        given:
        ApplicationContext ctx = ApplicationContext.run(
                'kafka.enabled': true, 'spec.name': 'ConfigProductListenerTest'
        )

        when:
        Product product = new Product('Blue Trainers', 5)
        ProductClient client = ctx.getBean(ProductClient.class)
        client.receive('Nike', product)

        then:
        noExceptionThrown()
    }
}
