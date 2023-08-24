package io.micronaut.kafka.docs.producer.methods

import io.micronaut.configuration.kafka.annotation.KafkaClient
import io.micronaut.configuration.kafka.annotation.KafkaKey
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.context.annotation.Requires
import io.micronaut.messaging.annotation.MessageHeader
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.Headers

@Requires(property = "spec.name", value = "ProductClientTest")
@KafkaClient("product-client")
interface ProductClient {

    // tag::key[]
    @Topic("my-products")
    fun sendProduct(@KafkaKey brand: String, name: String)
    // end::key[]

    // tag::messageheader[]
    @Topic("my-products")
    fun sendProduct(@KafkaKey brand: String, name: String, @MessageHeader("My-Header") myHeader: String)
    // end::messageheader[]

    // tag::collectionheaders[]
    @Topic("my-bicycles")
    fun sendBicycle(@KafkaKey brand: String, model: String, headers: Collection<Header>)
    // end::collectionheaders[]

    // tag::kafkaheaders[]
    @Topic("my-bicycles")
    fun sendBicycle(@KafkaKey brand: String, model: String, headers: Headers)
    // end::kafkaheaders[]
}
