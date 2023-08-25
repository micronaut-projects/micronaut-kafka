package io.micronaut.kafka.docs.consumer.sendto

import io.micronaut.context.ApplicationContext
import io.micronaut.kafka.docs.Product
import spock.lang.Specification

class SendToProductListenerTest extends Specification {
    void "test Send Product"() {
        given:
        ApplicationContext ctx = ApplicationContext.run(
                'kafka.enabled': true, 'spec.name': 'SendToProductListenerTest'
        )

        when:
        Product product = new Product("Blue Trainers", 5)
        ProductClient client = ctx.getBean(ProductClient.class)
        client.send("Nike", product)

        then:
        noExceptionThrown()
    }
}
