package io.micronaut.kafka.docs.quickstart

// tag::imports[]
import io.micronaut.configuration.kafka.annotation.KafkaClient
import io.micronaut.configuration.kafka.annotation.KafkaKey
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.context.annotation.Requires
// end::imports[]

@Requires(property = "spec.name", value = "QuickstartTest")
// tag::clazz[]
@KafkaClient // <1>
interface ProductClient {
    @Topic("my-products")  // <2>
    fun sendProduct(@KafkaKey brand: String, name: String) // <3>

    fun sendProduct(@Topic topic: String, @KafkaKey brand: String, name: String) // <4>
}
// end::clazz[]
