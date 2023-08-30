package io.micronaut.kafka.docs.consumer.config

// tag::imports[]
import io.micronaut.configuration.kafka.annotation.KafkaKey
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.context.annotation.Property
import io.micronaut.context.annotation.Requires
import io.micronaut.kafka.docs.Product
import io.micronaut.kafka.docs.consumer.topics.ProductListener
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory

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

    companion object {
        private val LOG = LoggerFactory.getLogger(ProductListener::class.java)
    }

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
        LOG.info("Got Product - {} by {}", product.name, brand)
    }
    // end::method[]

    // tag::consumeRecord[]
    @Topic("awesome-products")
    fun receive(record: ConsumerRecord<String, Product>) { // <1>
        val name = record.value() // <2>
        val brand = record.key() // <3>
        LOG.info("Got Product - $name by $brand")
    }
    // end::consumeRecord[]
}
