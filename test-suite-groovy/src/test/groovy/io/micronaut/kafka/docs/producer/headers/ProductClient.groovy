package io.micronaut.kafka.docs.producer.headers

// tag::imports[]
import io.micronaut.configuration.kafka.annotation.KafkaClient
import io.micronaut.messaging.annotation.MessageHeader
// end::imports[]

// tag::clazz[]
@KafkaClient(id='product-client')
@MessageHeader(name = 'X-Token', value = "${my.application.token}")
interface ProductClient {
    // define client API
}
// end::clazz[]
