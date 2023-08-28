package io.micronaut.kafka.docs.consumer.sendto

import io.micronaut.configuration.kafka.annotation.KafkaClient
import io.micronaut.configuration.kafka.annotation.KafkaKey
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.context.annotation.Requires
import io.micronaut.kafka.docs.Product;

@Requires(property = 'spec.name', value = 'SendToProductListenerTest')
@KafkaClient('product-client')
interface ProductClient {

    @Topic('sendto-products')
    void send(@KafkaKey String brand, Product product)
}
