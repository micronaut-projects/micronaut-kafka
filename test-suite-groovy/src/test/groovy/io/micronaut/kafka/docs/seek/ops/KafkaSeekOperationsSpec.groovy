package io.micronaut.kafka.docs.seek.ops

import io.micronaut.configuration.kafka.annotation.KafkaClient
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.context.annotation.Property
import io.micronaut.context.annotation.Requires
import io.micronaut.kafka.docs.AbstractKafkaTest
import io.micronaut.kafka.docs.Product
import io.micronaut.test.extensions.spock.annotation.MicronautTest
import jakarta.inject.Inject
import spock.util.concurrent.PollingConditions

@MicronautTest
@Property(name = "spec.name", value = "KafkaSeekOperationsSpec")
class KafkaSeekOperationsSpec extends AbstractKafkaTest {

    @Inject
    ProductClient producer

    @Inject
    ProductListener consumer

    void "test product listener"() {
        given:
        Product product0 = new Product("Apple", 10)
        Product product1 = new Product("Banana", 20)

        when:
        producer.produce(product0)
        producer.produce(product1)

        then:
        new PollingConditions(timeout: 5).eventually {
            consumer.processed.contains(product0)
            !consumer.processed.contains(product1)
        }

        cleanup:
        MY_KAFKA.stop()
    }

    @Requires(property = "spec.name", value = "KafkaSeekOperationsSpec")
    @KafkaClient
    static interface ProductClient {
        @Topic("awesome-products")
        void produce(Product product)
    }
}
