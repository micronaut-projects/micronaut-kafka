package io.micronaut.kafka.docs.consumer.config

// tag::imports[]
import io.micronaut.configuration.kafka.annotation.KafkaKey
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.context.annotation.Property
import io.micronaut.context.annotation.Requires
import io.micronaut.kafka.docs.Product
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
// end::imports[]

@Requires(property = "spec.name", value = "ConfigProductListenerTest")
// tag::clazz[]
@KafkaListener(
    groupId = "products",
    pollTimeout = "500ms",
    properties = [Property(name = ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, value = "10000")]
)
class ProductListener {
// end::clazz[]

    // tag::method[]
    @Topic("awesome-products")
    fun receive(
        @KafkaKey brand: String,  // <1>
        product: Product,  // <2>
        offset: Long,  // <3>
        partition: Int,  // <4>
        topic: String?,  // <5>
        timestamp: Long // <6>
    ) {
        println("Got Product - $product.name by $brand")
    }
    // end::method[]

    // tag::consumeRecord[]
    @Topic("awesome-products")
    fun receive(record: ConsumerRecord<String, Product>) { // <1>
        val name = record.value() // <2>
        val brand = record.key() // <3>
        println("Got Product - $name by $brand")
    }
    // end::consumeRecord[]
}
